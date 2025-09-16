#!/usr/bin/env python3
import argparse, os, gzip, math, time, gc, sys, glob, re
import pyarrow.parquet as pq
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter
try:
    import psutil
except Exception:
    psutil = None

US_NAMES = ['usa']
CN_NAMES = ['peoples r china']
EU_NAMES = ['eu']  # EU already normalized upstream

def discover_resume(out_prefix, global_part_id):
    pat = f"{out_prefix}.part{global_part_id:04d}.chunk*.csv.gz"
    have = set()
    rx = re.compile(r"\.chunk(\d{5})\.csv\.gz$")
    for p in glob.glob(pat):
        m = rx.search(p)
        if m: have.add(int(m.group(1)))
    k = 0
    while k in have:
        k += 1
    # Log resume info for clarity
    if have:
        print(f"[resume] part={global_part_id} found chunks {sorted(have)}, resuming from {k}")
    else:
        print(f"[resume] part={global_part_id} no chunks found, starting fresh")
    return k  # first missing chunk index

def shard_slice(n, global_part_id, global_total_parts):
    per = n // global_total_parts
    start = global_part_id * per
    end = n if global_part_id == global_total_parts - 1 else start + per
    return start, end

def write_chunk(path_prefix, global_part_id, chunk_idx, rows):
    out_dir = os.path.dirname(path_prefix)
    os.makedirs(out_dir, exist_ok=True)
    final_path = f"{path_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
    tmp_path   = final_path + ".tmp"
    with gzip.open(tmp_path, 'wt', compresslevel=1) as f:
        f.write("UID\tcd\tcd_only_us\tcd_excl_us\tcd_only_eu\tcd_excl_eu\tcd_only_cn\tcd_excl_cn\n")
        for row in rows:
            f.write("\t".join("NaN" if (isinstance(x, float) and math.isnan(x)) else str(x) for x in row) + "\n")
    os.replace(tmp_path, final_path)
    return final_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cache-dir', required=True)
    ap.add_argument('--countries-parquet', required=True)
    ap.add_argument('--region-cache-dir', required=True)
    ap.add_argument('--out-prefix', required=True)  # prefix for chunked outputs
    ap.add_argument('--years', type=int, default=5)
    # per-cluster and per-array sharding
    ap.add_argument('--total-parts', type=int, default=1, help='parts per cluster (array size)')
    ap.add_argument('--part-id', type=int, default=0, help='0..total-parts-1 within the cluster')
    ap.add_argument('--clusters', type=int, default=1, help='number of clusters/partitions')
    ap.add_argument('--cluster-index', type=int, default=0, help='0..clusters-1 which cluster this run is')
    ap.add_argument('--log-every', type=int, default=200000)
    ap.add_argument('--chunk-size', type=int, default=340000, help='rows per gz chunk')
    # optional: year bitmap cache directory
    ap.add_argument('--year-bitmap-dir', default=None)
    ap.add_argument('--flip-edges', action='store_true',
                    help='Input edges are (cited,citing); flip to (citing,cited) at ingest')
    ap.add_argument('--safe-mode', action='store_true',
                    help='Use safe mode for debugging')
    ap.add_argument('--preflight', action='store_true',
                    help='Run a small cdindex smoke test before the loop')
    ap.add_argument('--first-chunk', type=int, default=None,
                    help='Start at this chunk index within the part (overrides resume discovery)')
    ap.add_argument('--num-chunks', type=int, default=None,
                    help='Process at most this many chunks then exit')
    args = ap.parse_args()

    # print("ABI cookie:", hex(_cdindex.abi_cookie()), "from", _cdindex.__file__, flush=True)

    if args.safe_mode:
        os.environ['CDINDEX_SAFE_MODE'] = '1'

    vpath = os.path.join(args.cache_dir, 'paper_years.parquet')
    epath = os.path.join(args.cache_dir, 'edges.parquet')

    t0 = time.perf_counter()
    def mem(msg):
        if psutil:
            rss = psutil.Process().memory_info().rss / (1024*1024)
            print(f"[mem] {msg}: {rss:,.1f} MB", flush=True)
        else:
            print(f"[mem] {msg}: N/A", flush=True)

    # 1) Load vertices (UID->id captured)
    vt = pq.read_table(vpath, columns=['paper_id','UID','year'])
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vt)
    mem("after add_vertices_from_arrow")

    # 2) Load edges (UID columns supported in C++)
    import pyarrow as pa, pyarrow.dataset as ds, gc, math
    cols = ['source_id','target_id','source_uid','target_uid']  # whatever exists
    dataset = ds.dataset(epath, format="parquet")
    scanner = dataset.scanner(columns=[c for c in cols if c in dataset.schema.names],
                            use_threads=True,
                            batch_size=1<<20)  # tune (e.g., 1–8MB)

    if args.flip_edges:
        g.set_flip_edge_direction_on_ingest(True)
    batch_buf = []
    B = 0
    for i, batch in enumerate(scanner.to_batches()):
        batch_buf.append(batch)
        B += batch.num_rows
        # Coalesce a few batches to amortize overhead
        if B >= 5_000_000:  # tune by memory; ~5M edges per push works well
            g.add_edges_from_arrow(pa.Table.from_batches(batch_buf))
            batch_buf.clear(); B = 0
            if (i+1) % 20 == 0:
                gc.collect()
    if batch_buf:
        g.add_edges_from_arrow(pa.Table.from_batches(batch_buf))
    mem("after add_edges_from_arrow")
    del dataset; gc.collect()
    del scanner; gc.collect()
    del batch_buf; gc.collect()

    # 3) Prepare and build year bitmaps
    g.prepare_for_searching()
    mem("after prepare_for_searching")

    if args.year_bitmap_dir and os.path.isdir(args.year_bitmap_dir):
        # try loading if present, else build from vertices
        try:
            g.load_year_bitmaps(args.year_bitmap_dir)
            print(f"Loaded year bitmaps from {args.year_bitmap_dir}")
        except Exception:
            g.properties.ingest_arrow(vt)
            g.properties.build_indexes()
            os.makedirs(args.year_bitmap_dir, exist_ok=True)
            try:
                g.save_year_bitmaps(args.year_bitmap_dir)
            except Exception:
                pass
    else:
        g.properties.ingest_arrow(vt)
        g.properties.build_indexes()
    mem("after year bitmaps (ingest/build or load)")

    # 4) Regions: load or build once, then persist
    loaded = False
    # register normalized country names
    g.set_country_lists(US_NAMES, CN_NAMES, EU_NAMES)
    try:
        g.load_region_bitmaps(args.region_cache_dir)
        loaded = True
        us_sz, eu_sz, cn_sz = g.region_sizes()
        print(f"Loaded region bitmaps from cache: {args.region_cache_dir}  "
              f"(cardinalities: US={us_sz:,}, EU={eu_sz:,}, CN={cn_sz:,})")
        if (us_sz + eu_sz + cn_sz) == 0:
            print("[warn] Loaded region cache is empty; will rebuild from countries parquet.")
            loaded = False
    except Exception as e:
        print(f"[warn] load_region_bitmaps failed: {e}")
        loaded = False

    if not loaded:
        ct = pq.read_table(args.countries_parquet, columns=['UID','country'])
        g.ingest_countries_from_parquet(ct, 'UID', 'country')
        os.makedirs(args.region_cache_dir, exist_ok=True)
        g.save_region_bitmaps(args.region_cache_dir)
        print("Built & saved region bitmaps to:", args.region_cache_dir)
        us_sz, eu_sz, cn_sz = g.region_sizes()
        print(f"Built & saved region bitmaps to: {args.region_cache_dir}  "
              f"(cardinalities: US={us_sz:,}, EU={eu_sz:,}, CN={cn_sz:,})")
    mem("after regions ready")

    # quick sanity: file sizes and Arrow counts by label
    sizes = {f: (os.path.getsize(os.path.join(args.region_cache_dir, f))
                if os.path.exists(os.path.join(args.region_cache_dir, f)) else 0)
            for f in ("us.roar","eu.roar","cn.roar")}
    print("[sanity] region file sizes (bytes):", sizes, flush=True)
    if not loaded:
        try:
            import pyarrow.compute as pc
            lower = pc.utf8_lower(ct['country'])
            def cnt(name):
                return int(pc.sum(pc.cast(pc.equal(lower, pc.scalar(name)), 'int64')).as_py())
            print("[sanity] country rows:", {"usa": cnt("usa"),
                                            "eu": cnt("eu"),
                                            "peoples r china": cnt("peoples r china")}, flush=True)
        except Exception as e:
            print("[sanity] label count failed:", e, flush=True)
        del ct; gc.collect()

    # 5) Sharding across clusters and arrays
    n = vt.num_rows
    global_total_parts = args.total_parts * args.clusters
    global_part_id = args.cluster_index * args.total_parts + args.part_id
    start, end = shard_slice(n, global_part_id, global_total_parts)
    print(f"[shard] global_part_id={global_part_id} total_parts={global_total_parts}  rows=[{start}:{end})  count={end-start}", flush=True)

    # Slice *both* columns from the same row range so order aligns without any mapping.
    # This assumes vertices.parquet rows are in the same deterministic order used for id assignment.
    # If UID column is not present, we'll fall back to an empty list and write only paper_id.
    paper_ids = vt.column('paper_id').slice(start, end-start).to_pylist()
    uids = vt.column('UID').slice(start, end-start).to_pylist()
    
    # Compute part size and chunk math
    part_rows = end - start
    chunks_in_part = (part_rows + args.chunk_size - 1) // args.chunk_size

    # Choose starting chunk index
    if args.first_chunk is not None:
        chunk_idx = args.first_chunk
    else:
        # auto-resume: start from first missing chunk on disk
        chunk_idx = discover_resume(args.out_prefix, global_part_id)

    if chunk_idx >= chunks_in_part:
        print(f"[resume] part={global_part_id} already complete (chunk_idx={chunk_idx} >= {chunks_in_part})")
        return

    # Optional cap on how many chunks to process in this run
    max_chunks_to_do = args.num_chunks if args.num_chunks is not None else (chunks_in_part - chunk_idx)

    # Compute row skip within this part for the starting chunk
    rows_to_skip = chunk_idx * args.chunk_size
    print(f"[plan] part={global_part_id} rows=[{start}:{end})  chunks={chunks_in_part}  "
          f"starting_chunk={chunk_idx}  take_chunks={max_chunks_to_do}  skip_rows={rows_to_skip}", flush=True)

    # Apply the skip to our local lists
    paper_ids = paper_ids[rows_to_skip:]
    uids      = uids[rows_to_skip:]
    total     = len(paper_ids)
    
    # 6) Now it's safe to free the UID mapping + pinned UID Arrow buffers
    # (after extracting all UID data we need)
    g.clear_uid_map()
    mem("after clear_uid_map")
    years = args.years
    print(f"[config] years={years}  flip_edges={args.flip_edges}", flush=True)

    # Preflight: catch binding/type issues immediately
    # try:
    #     test_pid = int(paper_ids[0])
    #     print("preflight:", g.debug_counts(test_pid, years))  # quick path sanity
    #     test = g.cdindex_all(test_pid, args.years)
    #     print("preflight cdindex_all:", test)
    #     # assert isinstance(test, (list, tuple)) and len(test) == 7
    # except Exception as e:
    #     print("[fatal] cdindex_all smoke test failed:", repr(e), flush=True)
    #     sys.exit(1)

    # Pick a focal with at least some structure
    pid0 = None
    for pid in vt.column('paper_id').slice(start, min(1000, end-start)).to_pylist():
        ts = g.get_timestamp(pid)
        if ts and (g.in_degree(pid) or g.out_degree(pid)):
            pid0 = int(pid); break
    if pid0 is None:
        pid0 = int(paper_ids[0])

    print("preflight ids:", pid0, "vc=", g.vertex_count(), "ec=", g.edge_count(), "ts=", g.get_timestamp(pid0), flush=True)

    # Verification checks removed for performance (segfault issue resolved)

    if args.preflight:
        # Light call first (no region math)
        _ = g.cdindex(pid0, years)
        # Then the heavy one
        test = g.cdindex_all(pid0, years)
        print("preflight cdindex_all:", test, flush=True)

    # 7) Compute with chunked writes
    bad = 0
    out_rows = []
    chunk_written = 0
    t_start_loop = time.perf_counter()
    for idx, (pid, uid) in enumerate(zip(paper_ids, uids), 1):
        try:
            (cd, cd_only_us, cd_excl_us,
             cd_only_eu, cd_excl_eu,
             cd_only_cn, cd_excl_cn) = g.cdindex_all(pid, years)
        except Exception as e:
            # defensive: if one paper fails, emit NaNs for it
            bad += 1
            if bad <= 5:
                print("[warn] cdindex_all failed for", pid, repr(e), flush=True)
                print(f"Paper ID: {pid}")
                print(f"Error: {e}")
            if bad > 1000:
                print("[fatal] too many per-paper failures; aborting", flush=True)
                sys.exit(2)
            cd = float('nan'); cd_only_us = cd_excl_us = cd_only_eu = cd_excl_eu = cd_only_cn = cd_excl_cn = float('nan')

        out_rows.append((uid, cd, cd_only_us, cd_excl_us,
                         cd_only_eu, cd_excl_eu, cd_only_cn, cd_excl_cn))

        if idx % args.log_every == 0:
            elapsed = time.perf_counter() - t_start_loop
            rate = idx / max(1e-9, elapsed)
            print(f"[cluster {args.cluster_index+1}/{args.clusters} | part {args.part_id+1}/{args.total_parts}] "
                  f"processed {idx:,} / {total:,}  ({rate:,.1f} papers/sec)", flush=True)
            mem("progress")

        if len(out_rows) >= args.chunk_size:
            path = write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
            print(f"wrote chunk: {path}", flush=True)
            out_rows.clear()
            chunk_idx += 1
            chunk_written += 1
            gc.collect()
            if chunk_written >= max_chunks_to_do:
                print(f"[plan] Completed assigned {chunk_written} chunks for part {global_part_id}. Exiting.", flush=True)
                break

    if out_rows and chunk_written < max_chunks_to_do:
        path = write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
        print(f"wrote final (partial) chunk: {path}", flush=True)

    elapsed = time.perf_counter() - t0
    print(f"Total time: {elapsed:,.1f} sec", flush=True)
    mem("final")

    g.print_benchmark_summary()
    
if __name__ == '__main__':
    main()
