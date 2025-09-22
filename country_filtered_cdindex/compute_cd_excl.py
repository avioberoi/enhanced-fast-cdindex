#!/usr/bin/env python3
import argparse, os, gzip, math, time, sys, glob, signal, pickle
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path
import pyarrow as pa
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
    final_path = f"{path_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
    tmp_path   = final_path + ".tmp"
    with gzip.open(tmp_path, 'wt', compresslevel=1) as f:
        f.write("UID\tcd_excl\n")
        for uid, val in rows:
            if isinstance(val, float) and math.isnan(val):
                f.write(f"{uid}\tNaN\n")
            else:
                f.write(f"{uid}\t{val:g}\n")  # Use :g format for cleaner float output
    os.replace(tmp_path, final_path)
    print(f"wrote chunk: {final_path}", flush=True)

def region_to_filter(ex):
    ex = ex.strip().lower()
    if ex == "us":
        return CiterFilter.ExcludeUS
    if ex == "eu":
        return CiterFilter.ExcludeEU
    if ex == "cn":
        return CiterFilter.ExcludeCN
    raise ValueError(f"Unknown exclude region: {ex}")

def load_global_processed_uids(out_prefix):
    """
    Load the global processed UIDs file created by the discovery phase.
    Returns empty set if file doesn't exist.
    """
    global_file = Path(out_prefix).parent / "global_processed_uids.pkl"
    
    if not global_file.exists():
        print(f"[global] No global processed file found at {global_file}")
        print(f"[global] Starting from scratch (no UIDs to skip)")
        return set()
    
    try:
        with open(global_file, 'rb') as f:
            processed_uids = pickle.load(f)
        
        print(f"[global] Loaded {len(processed_uids):,} processed UIDs from {global_file}")
        return processed_uids
        
    except Exception as e:
        print(f"[global] Warning: Could not load global processed UIDs: {e}")
        print(f"[global] Starting from scratch")
        return set()

# Global termination flag
_terminate_requested = False

def _sigterm_handler(signum, frame):
    global _terminate_requested
    print(f"[signal] Received signal {signum}, requesting graceful shutdown...", flush=True)
    _terminate_requested = True

def main():
    global _terminate_requested
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    
    parser = argparse.ArgumentParser(description="Compute CD exclusion scores")
    parser.add_argument("--cache-dir", required=True, help="Directory for cached graph data")
    parser.add_argument("--countries-parquet", required=True, help="Path to countries parquet file")
    parser.add_argument("--region-cache-dir", required=True, help="Directory for region bitmaps")
    parser.add_argument("--out-prefix", required=True, help="Output file prefix")
    parser.add_argument("--exclude-region", required=True, choices=["us", "eu", "cn"], help="Region to exclude")
    parser.add_argument("--years", type=int, default=5, help="Number of years for citation window")
    parser.add_argument("--total-parts", type=int, required=True, help="Total number of array jobs")
    parser.add_argument("--part-id", type=int, required=True, help="This job's part ID (0-indexed)")
    parser.add_argument("--log-every", type=int, default=500000, help="Log progress every N papers")
    parser.add_argument("--chunk-size", type=int, default=340000, help="Papers per output chunk")
    parser.add_argument("--flip-edges", action="store_true", help="Flip citation edges")
    
    args = parser.parse_args()
    
    print(f"[config] years={args.years}  filter={region_to_filter(args.exclude_region)}  flip_edges={args.flip_edges}", flush=True)
    
    # Load global processed UIDs from discovery phase
    global_processed_uids = load_global_processed_uids(args.out_prefix)
    
    # Load graph
    print(f"[graph] Loading graph from {args.cache_dir}...", flush=True)
    vpath = os.path.join(args.cache_dir, "paper_years.parquet")
    epath = os.path.join(args.cache_dir, "edges.parquet")

    # 1) Load vertices
    vt = pq.read_table(vpath, columns=['paper_id','UID','year'])
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vt)

    # 2) Stream edges directly (memory efficient for 1.5B edges)
    if args.flip_edges:
        g.set_flip_edge_direction_on_ingest(True)
    
    dataset = ds.dataset(epath, format="parquet")
    cols = ['source_id','target_id','source_uid','target_uid']
    available_cols = [c for c in cols if c in dataset.schema.names]
    scanner = dataset.scanner(columns=available_cols, batch_size=1<<20)
    
    for batch in scanner.to_batches():
        # Convert RecordBatch to Table for the API
        table = pa.Table.from_batches([batch])
        g.add_edges_from_arrow(table)
    
    del dataset, scanner

    # 3) Prepare + year bitmaps
    g.prepare_for_searching()
    g.properties.ingest_arrow(vt)
    g.properties.build_indexes()

    # 4) Regions: load or build
    g.set_country_lists(US_NAMES, CN_NAMES, EU_NAMES)
    loaded = False
    try:
        g.load_region_bitmaps(args.region_cache_dir)
        loaded = True
        us_sz, eu_sz, cn_sz = g.region_sizes()
        print(f"[regions] US={us_sz:,} EU={eu_sz:,} CN={cn_sz:,}", flush=True)
        if (us_sz + eu_sz + cn_sz) == 0:
            print("[warn] Loaded region cache is empty; will rebuild from countries parquet.", flush=True)
            loaded = False
    except Exception as e:
        print(f"[warn] load_region_bitmaps failed: {e}", flush=True)
        loaded = False

    if not loaded:
        ct = pq.read_table(args.countries_parquet, columns=['UID','country'])
        g.ingest_countries_from_parquet(ct, 'UID', 'country')
        os.makedirs(args.region_cache_dir, exist_ok=True)
        g.save_region_bitmaps(args.region_cache_dir)
        us_sz, eu_sz, cn_sz = g.region_sizes()
        print(f"[regions] US={us_sz:,} EU={eu_sz:,} CN={cn_sz:,}", flush=True)
        del ct

    # Get total papers and compute simple shard
    total_papers = vt.num_rows
    start, end = shard_slice(total_papers, args.part_id, args.total_parts)
    
    print(f"[shard] Part {args.part_id}/{args.total_parts}: rows [{start}:{end}) = {end-start:,} papers")
    print(f"[global] Will skip {len(global_processed_uids):,} already-processed UIDs", flush=True)
    
    # Extract our shard and filter efficiently
    shard_table = vt.slice(start, end - start)
    
    # Process shard in smaller chunks to minimize memory usage
    work_items = []
    papers_skipped = 0
    chunk_size = 100000  # Process 100k rows at a time
    
    for chunk_start in range(0, len(shard_table), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(shard_table))
        chunk = shard_table.slice(chunk_start, chunk_end - chunk_start)
        
        uids = chunk.column('UID')
        pids = chunk.column('paper_id')
        
        for i in range(len(uids)):
            uid = str(uids[i].as_py())
            if uid in global_processed_uids:
                papers_skipped += 1
            else:
                work_items.append((uid, pids[i].as_py()))
        
        del chunk, uids, pids  # Free chunk memory immediately
    
    print(f"[filter] {len(work_items):,} papers to process, {papers_skipped:,} already done", flush=True)
    
    # Clear memory
    g.clear_uid_map()
    del vt, shard_table
    
    # Process work items
    out_rows = []
    chunk_idx = 0
    processed_count = 0
    total_work = len(work_items)
    
    for uid, pid in work_items:
        if _terminate_requested:
            break
            
        try:
            # Compute CD exclusion score
            cd_score = g.cdindex_filtered(pid, args.years, region_to_filter(args.exclude_region))
            out_rows.append((uid, cd_score))
            processed_count += 1
            
            # Log progress
            if processed_count % args.log_every == 0:
                print(f"[progress] Part {args.part_id}: {processed_count:,}/{total_work:,} processed", flush=True)
            
            # Check if we should flush
            if len(out_rows) >= args.chunk_size:
                # Write chunk
                write_chunk(args.out_prefix, args.part_id, chunk_idx, out_rows)
                out_rows.clear()  # More explicit memory clearing
                chunk_idx += 1

        except Exception as e:
            print(f"[error] Failed to process paper {pid} (UID {uid}): {e}", flush=True)
            continue
    
    # Clear work_items from memory
    del work_items
    
    # Final flush
    if out_rows and not _terminate_requested:
        print(f"[final] Writing final chunk with {len(out_rows)} papers...", flush=True)
        write_chunk(args.out_prefix, args.part_id, chunk_idx, out_rows)
        chunk_idx += 1
    
    # Handle graceful shutdown
    if _terminate_requested and out_rows:
        print(f"[sigterm] Writing partial chunk with {len(out_rows)} papers before shutdown...", flush=True)
        write_chunk(args.out_prefix, args.part_id, chunk_idx, out_rows)
    
    total_chunks = chunk_idx + (1 if out_rows and not _terminate_requested else 0)
    print(f"[complete] Part {args.part_id}: {total_work:,} papers assigned, {papers_skipped:,} skipped, {total_chunks} chunks written", flush=True)
    
    if _terminate_requested:
        print(f"[sigterm] Part {args.part_id}: Graceful shutdown completed", flush=True)
        sys.exit(0)
    else:
        print(f"[success] Part {args.part_id}: Job completed successfully", flush=True)

if __name__ == "__main__":
    main()
