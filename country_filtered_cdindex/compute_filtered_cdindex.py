#!/usr/bin/env python3
import argparse, os, gzip, math
import pyarrow.parquet as pq
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter

US_NAMES = ['usa']
CN_NAMES = ['peoples r china']
EU_NAMES = ['eu']  # EU already normalized upstream

def shard_slice(n, global_part_id, global_total_parts):
    per = n // global_total_parts
    start = global_part_id * per
    end = n if global_part_id == global_total_parts - 1 else start + per
    return start, end

def write_chunk(path_prefix, global_part_id, chunk_idx, rows):
    out_dir = os.path.dirname(path_prefix)
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{path_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
    with gzip.open(out_path, 'wt') as f:
        f.write("paper_id\tcd\tcd_only_us\tcd_excl_us\tcd_only_eu\tcd_excl_eu\tcd_only_cn\tcd_excl_cn\n")
        for row in rows:
            f.write("\t".join("NaN" if (isinstance(x, float) and math.isnan(x)) else str(x) for x in row) + "\n")
    return out_path

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
    args = ap.parse_args()

    vpath = os.path.join(args.cache_dir, 'paper_years.parquet')
    epath = os.path.join(args.cache_dir, 'edges.parquet')

    # 1) Load vertices (UID->id captured)
    vt = pq.read_table(vpath)
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vt)

    # 2) Load edges (UID columns supported in C++)
    et = pq.read_table(epath)
    g.add_edges_from_arrow(et)

    # 3) Prepare and build year bitmaps
    g.prepare_for_searching()
    if args.year_bmp_dir == args.year_bitmap_dir:
        # try loading if present, else build from vertices
        try:
            g.load_year_bitmaps(args.year_bmp_dir)
            print(f"Loaded year bitmaps from {args.year_bmp_dir}")
        except Exception:
            g.properties.ingest_arrow(vt)
            g.properties.build_indexes()
            os.makedirs(args.year_bmp_dir, exist_ok=True)
            try:
                g.save_year_bitmaps(args.year_bmp_dir)
            except Exception:
                pass
    else:
        g.properties.ingest_arrow(vt)
        g.properties.build_indexes()

    # 4) Regions: load or build once, then persist
    loaded = False
    try:
        g.load_region_bitmaps(args.region_cache_dir)
        loaded = True
        print("Loaded region bitmaps from cache:", args.region_cache_dir)
    except Exception:
        pass

    if not loaded:
        ct = pq.read_table(args.countries_parquet, columns=['UID','country'])
        g.set_country_lists(US_NAMES, CN_NAMES, EU_NAMES)  # register normalized names
        g.ingest_countries_from_parquet(ct, 'UID', 'country')
        os.makedirs(args.region_cache_dir, exist_ok=True)
        g.save_region_bitmaps(args.region_cache_dir)
        print("Built & saved region bitmaps to:", args.region_cache_dir)
        # After saving region bitmaps
        # g.clear_uid_map()

    # 5) Sharding across clusters and arrays
    n = vt.num_rows
    global_total_parts = args.total_parts * args.clusters
    global_part_id = args.cluster_index * args.total_parts + args.part_id
    start, end = shard_slice(n, global_part_id, global_total_parts)
    paper_ids = vt.column('paper_id').slice(start, end-start).to_pylist()
    years = args.years

    # 6) Compute with chunked writes
    out_rows, chunk_idx = [], 0
    total = len(paper_ids)
    for idx, pid in enumerate(paper_ids, 1):
        try:
            cd = g.cdindex(pid, years)
            cd_only_us = g.cdindex_filtered(pid, years, CiterFilter.OnlyUS)
            cd_excl_us = g.cdindex_filtered(pid, years, CiterFilter.ExcludeUS)
            cd_only_eu = g.cdindex_filtered(pid, years, CiterFilter.OnlyEU)
            cd_excl_eu = g.cdindex_filtered(pid, years, CiterFilter.ExcludeEU)
            cd_only_cn = g.cdindex_filtered(pid, years, CiterFilter.OnlyCN)
            cd_excl_cn = g.cdindex_filtered(pid, years, CiterFilter.ExcludeCN)
        except Exception:
            # defensive: if one paper fails, emit NaNs for it
            cd = float('nan'); cd_only_us = cd_excl_us = cd_only_eu = cd_excl_eu = cd_only_cn = cd_excl_cn = float('nan')

        out_rows.append((pid, cd, cd_only_us, cd_excl_us,
                         cd_only_eu, cd_excl_eu, cd_only_cn, cd_excl_cn))

        if idx % args.log_every == 0:
            print(f"[cluster {args.cluster_index+1}/{args.clusters} | part {args.part_id+1}/{args.total_parts}] "
                  f"processed {idx:,} / {total:,}")

        if len(out_rows) >= args.chunk_size:
            path = write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
            print(f"wrote chunk: {path}")
            out_rows.clear()
            chunk_idx += 1

    if out_rows:
        path = write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
        print(f"wrote final chunk: {path}")

if __name__ == '__main__':
    main()
