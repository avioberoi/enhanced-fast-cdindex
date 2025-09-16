#!/usr/bin/env python3
import argparse, os, gzip, math, time, sys, glob, signal, pickle
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path
import pyarrow as pa
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter

try:
    import xxhash
    HASH_AVAILABLE = True
except ImportError:
    import zlib
    HASH_AVAILABLE = False

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

class IndependentJobResume:
    """
    Independent job resume tracking - each array job manages its own state.
    No shared database, no concurrency issues.
    """
    
    def __init__(self, out_prefix, part_id):
        self.out_prefix = out_prefix
        self.part_id = part_id
        
        # Create job-specific resume directory and file
        self.resume_dir = Path(out_prefix).parent / f".resume_part_{part_id}"
        self.resume_dir.mkdir(parents=True, exist_ok=True)
        
        self.processed_file = self.resume_dir / "processed_uids.pkl"
        self.progress_file = self.resume_dir / "progress.txt"
        
        # Load existing processed UIDs
        self.processed_uids = self._load_processed_uids()
        print(f"[resume] Part {part_id}: Loaded {len(self.processed_uids)} processed UIDs", flush=True)
    
    def _load_processed_uids(self):
        """Load previously processed UIDs for this job."""
        if self.processed_file.exists():
            try:
                with open(self.processed_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"[resume] Warning: Could not load processed UIDs: {e}", flush=True)
                return set()
        return set()
    
    def _save_processed_uids(self):
        """Save processed UIDs to disk."""
        tmp_file = str(self.processed_file) + ".tmp"
        try:
            with open(tmp_file, 'wb') as f:
                pickle.dump(self.processed_uids, f)
            os.replace(tmp_file, self.processed_file)
        except Exception as e:
            print(f"[resume] Warning: Could not save processed UIDs: {e}", flush=True)
    
    def filter_unprocessed(self, uids_batch):
        """Return UIDs that haven't been processed by this job."""
        return [uid for uid in uids_batch if str(uid) not in self.processed_uids]
    
    def add_uids(self, uids):
        """Mark UIDs as processed."""
        for uid in uids:
            self.processed_uids.add(str(uid))
        self._save_processed_uids()
    
    def get_processed_count(self):
        """Get count of processed UIDs for this job."""
        return len(self.processed_uids)
    
    def save_progress(self, chunk_count, total_processed):
        """Save progress information."""
        try:
            with open(self.progress_file, 'w') as f:
                f.write(f"chunks_written: {chunk_count}\n")
                f.write(f"uids_processed: {total_processed}\n")
                f.write(f"last_update: {time.time()}\n")
        except Exception as e:
            print(f"[resume] Warning: Could not save progress: {e}", flush=True)
    
    def get_next_chunk_index(self):
        """Find the next chunk index to write."""
        pattern = f"{self.out_prefix}.part{self.part_id:04d}.chunk*.csv.gz"
        existing_chunks = glob.glob(pattern)
        
        if not existing_chunks:
            return 0
        
        # Extract chunk numbers
        chunk_nums = []
        for path in existing_chunks:
            try:
                # Extract chunk number from filename
                basename = os.path.basename(path)
                chunk_part = basename.split('.chunk')[1].split('.csv.gz')[0]
                chunk_nums.append(int(chunk_part))
            except (IndexError, ValueError):
                continue
        
        return max(chunk_nums) + 1 if chunk_nums else 0

def stable_hash_partition(uid, total_parts):
    """Deterministic hash partitioning for UIDs."""
    if HASH_AVAILABLE:
        hash_val = xxhash.xxh64(str(uid)).intdigest()
    else:
        hash_val = zlib.adler32(str(uid).encode())
    
    return hash_val % total_parts

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
    parser.add_argument("--num-chunks", type=int, help="Max chunks to process (for testing)")
    
    args = parser.parse_args()
    
    print(f"[config] years={args.years}  filter={region_to_filter(args.exclude_region)}  flip_edges={args.flip_edges}", flush=True)
    
    # Initialize independent resume tracking
    resume_tracker = IndependentJobResume(args.out_prefix, args.part_id)
    
    # Load graph
    print(f"[graph] Loading graph from {args.cache_dir}...", flush=True)
    vpath = os.path.join(args.cache_dir, "paper_years.parquet")
    epath = os.path.join(args.cache_dir, "edges.parquet")
    
    # 1) Load vertices
    vt = pq.read_table(vpath, columns=['paper_id','UID','year'])
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vt)

    # 2) Load edges via dataset scanner (column-flexible; supports *_uid)
    dataset = ds.dataset(epath, format="parquet")
    cols = ['source_id','target_id','source_uid','target_uid']
    col_list = [c for c in cols if c in dataset.schema.names]
    scanner = dataset.scanner(columns=col_list, use_threads=True, batch_size=1<<20)

    if args.flip_edges:
        g.set_flip_edge_direction_on_ingest(True)

    batch_buf = []
    B = 0
    for i, batch in enumerate(scanner.to_batches()):
        batch_buf.append(batch)
        B += batch.num_rows
        if B >= 5_000_000:
            g.add_edges_from_arrow(pa.Table.from_batches(batch_buf))
            batch_buf.clear(); B = 0
    if batch_buf:
        g.add_edges_from_arrow(pa.Table.from_batches(batch_buf))
    del dataset, scanner, batch_buf

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

    # Get total papers from the vertex table before cleanup
    total_papers = vt.num_rows
    print(f"[run] Total papers in dataset: {total_papers:,}", flush=True)
    
    # Clear UID map and release vertex table memory early
    g.clear_uid_map()
    del vt
    
    # Print shard info (legacy - actual work is hash partitioned)
    start, end = shard_slice(total_papers, args.part_id, args.total_parts)
    print(f"[shard] global_part_id={args.part_id} total_parts={args.total_parts}  rows=[{start}:{end})  count={end-start}", flush=True)
    print(f"[plan] Note: hash partitioning overrides legacy shard slice; start/end shown only for reference", flush=True)
    
    # Get next chunk index for this job
    chunk_idx = resume_tracker.get_next_chunk_index()
    processed_count = resume_tracker.get_processed_count()
    
    print(f"[resume] Part {args.part_id}: Starting from chunk {chunk_idx}, {processed_count} UIDs already processed", flush=True)
    
    # Start streaming computation
    print(f"[compute] Starting fully streaming computation for part {args.part_id}/{args.total_parts}...", flush=True)
    print(f"[compute] Streaming from {vpath}...", flush=True)
    
    # Set up hash partitioning
    hash_lib = "xxhash" if HASH_AVAILABLE else "zlib.adler32"
    print(f"[compute] Using {hash_lib} for stable partitioning", flush=True)
    
    # Stream through data
    dataset = ds.dataset(vpath, format="parquet")
    scanner = dataset.scanner(columns=['UID', 'paper_id'], batch_size=1<<20, use_threads=True)
    
    out_rows = []
    chunks_done = 0
    max_chunks_to_do = args.num_chunks if args.num_chunks is not None else float('inf')
    
    print(f"[compute] Part {args.part_id} starting streaming computation", flush=True)
    
    for batch in scanner.to_batches():
        if _terminate_requested or chunks_done >= max_chunks_to_do:
            break
            
        # Extract UIDs and paper_ids
        uids = [str(u) for u in batch.column('UID').to_pylist()]
        paper_ids = batch.column('paper_id').to_pylist()
        
        # Hash partition: only process UIDs assigned to this job
        my_data = []
        for uid, pid in zip(uids, paper_ids):
            if stable_hash_partition(uid, args.total_parts) == args.part_id:
                my_data.append((uid, pid))
        
        if not my_data:
            continue
        
        # Filter out already processed UIDs
        batch_uids = [uid for uid, _ in my_data]
        unprocessed_uids = resume_tracker.filter_unprocessed(batch_uids)
        
        if not unprocessed_uids:
            continue
        
        # Create mapping for unprocessed UIDs
        unprocessed_set = set(unprocessed_uids)
        unprocessed_data = [(uid, pid) for uid, pid in my_data if uid in unprocessed_set]
        
        # Process unprocessed UIDs
        for uid, pid in unprocessed_data:
            if _terminate_requested or chunks_done >= max_chunks_to_do:
                break
                
            try:
                # Compute CD exclusion score
                cd_score = g.cdindex_filtered(pid, region_to_filter(args.exclude_region))
                out_rows.append((uid, cd_score))
                
                # Check if we should flush
                if len(out_rows) >= args.chunk_size:
                    # Write chunk
                    write_chunk(args.out_prefix, args.part_id, chunk_idx, out_rows)
                    
                    # Update resume tracking
                    uids_written = [uid for uid, _ in out_rows]
                    resume_tracker.add_uids(uids_written)
                    resume_tracker.save_progress(chunk_idx + 1, resume_tracker.get_processed_count())
                    
                    # Reset for next chunk
                    out_rows = []
                    chunk_idx += 1
                    chunks_done += 1
                    
                    if chunks_done >= max_chunks_to_do:
                        print(f"[limit] Reached max chunks limit ({args.num_chunks}), stopping", flush=True)
                        break
                        
            except Exception as e:
                print(f"[error] Failed to process paper {pid} (UID {uid}): {e}", flush=True)
                continue
    
    # Final flush
    if out_rows and not _terminate_requested:
        print(f"[final] Writing final chunk with {len(out_rows)} papers...", flush=True)
        write_chunk(args.out_prefix, args.part_id, chunk_idx, out_rows)
        
        uids_written = [uid for uid, _ in out_rows]
        resume_tracker.add_uids(uids_written)
        resume_tracker.save_progress(chunk_idx + 1, resume_tracker.get_processed_count())
        chunks_done += 1
    
    # Handle graceful shutdown
    if _terminate_requested and out_rows:
        print(f"[sigterm] Writing partial chunk with {len(out_rows)} papers before shutdown...", flush=True)
        write_chunk(args.out_prefix, args.part_id, chunk_idx, out_rows)
        
        uids_written = [uid for uid, _ in out_rows]
        resume_tracker.add_uids(uids_written)
        resume_tracker.save_progress(chunk_idx + 1, resume_tracker.get_processed_count())
    
    final_processed = resume_tracker.get_processed_count()
    print(f"[complete] Part {args.part_id}: Processed {final_processed} total UIDs, wrote {chunks_done} chunks", flush=True)
    
    if _terminate_requested:
        print(f"[sigterm] Part {args.part_id}: Graceful shutdown completed", flush=True)
        sys.exit(0)
    else:
        print(f"[success] Part {args.part_id}: Job completed successfully", flush=True)

if __name__ == "__main__":
    main()
