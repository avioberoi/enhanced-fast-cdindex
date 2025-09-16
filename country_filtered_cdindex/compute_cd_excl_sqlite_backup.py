#!/usr/bin/env python3
import argparse, os, gzip, math, time, sys, glob, signal
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path
import pyarrow as pa
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter
import sqlite3

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

def region_to_filter(ex):
    ex = ex.strip().lower()
    if ex == "us":
        return CiterFilter.ExcludeUS
    if ex == "eu":
        return CiterFilter.ExcludeEU
    if ex == "cn":
        return CiterFilter.ExcludeCN
    raise ValueError(f"Unknown exclude region: {ex}")

class UidLedgerSQLite:
    """
    SQLite-backed UID ledger for atomic resume across concurrent array jobs.
    SQLite handles concurrent access much better than DuckDB for this use case.
    """
    
    def __init__(self, db_path, tmpdir=None, part_id=0):
        self.db_path = db_path
        self.part_id = part_id
        self.tmpdir = tmpdir
        
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize schema
        self._ensure_schema()
    
    def _get_connection(self):
        """Get SQLite connection with proper configuration for concurrent access"""
        con = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)  # autocommit mode
        
        # Try WAL mode with fallback
        try:
            mode = con.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode.lower() != "wal":
                print(f"[ledger] journal_mode={mode} (WAL unavailable); falling back", flush=True)
                # TRUNCATE is safer than DELETE on busy/shared FS
                con.execute("PRAGMA journal_mode=TRUNCATE")
        except sqlite3.Error as e:
            print(f"[ledger] WAL enable failed: {e}; using default journal", flush=True)
        
        # Apply other optimizations with error handling
        try: 
            con.execute("PRAGMA synchronous=NORMAL")
        except: 
            pass
        try: 
            con.execute("PRAGMA busy_timeout=30000")
        except: 
            pass
        try: 
            con.execute("PRAGMA temp_store=MEMORY")
        except: 
            pass
        
        return con
    
    def _ensure_schema(self):
        """Create tables and indexes if they don't exist"""
        con = self._get_connection()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    mtime REAL NOT NULL,
                    size INTEGER NOT NULL,
                    indexed_at REAL NOT NULL
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS processed (
                    uid TEXT PRIMARY KEY
                )
            """)
            # No need for explicit index - PRIMARY KEY already creates one
            # No need for explicit commit in autocommit mode
        finally:
            con.close()
    
    def _filter_unprocessed_batch(self, uids_batch):
        """Filter batch of UIDs against processed table, return processed UIDs"""
        if not uids_batch:
            return []
        
        con = self._get_connection()
        try:
            # Use IN clause for small batches, otherwise use temp table
            if len(uids_batch) <= 100:
                placeholders = ','.join('?' * len(uids_batch))
                result = con.execute(f"""
                    SELECT uid FROM processed WHERE uid IN ({placeholders})
                """, [str(u) for u in uids_batch]).fetchall()
            else:
                # For larger batches, use temp table
                con.execute("CREATE TEMP TABLE IF NOT EXISTS temp_batch(uid TEXT)")
                con.execute("DELETE FROM temp_batch")  # Clear any existing data
                con.executemany("INSERT INTO temp_batch(uid) VALUES (?)", [(str(u),) for u in uids_batch])
                result = con.execute("""
                    SELECT t.uid FROM temp_batch t JOIN processed p ON t.uid = p.uid
                """).fetchall()
            
            return [row[0] for row in result]
        finally:
            con.close()
    
    def add_uids(self, uids_iter, batch_size=50000):
        """Add UIDs to processed table in batches with deduplication"""
        buf = []
        
        def commit_batch(batch_uids):
            if not batch_uids:
                return
            
            con = self._get_connection()
            try:
                # Use explicit transaction for batch insert
                con.execute("BEGIN")
                con.executemany("INSERT OR IGNORE INTO processed (uid) VALUES (?)", 
                               [(str(u),) for u in batch_uids])
                con.execute("COMMIT")
            except:
                con.execute("ROLLBACK")
                raise
            finally:
                con.close()
        
        for u in uids_iter:
            buf.append(u)
            if len(buf) >= batch_size:
                commit_batch(buf)
                buf.clear()
        commit_batch(buf)
    
    def record_file(self, path):
        """Record file metadata after processing"""
        try:
            st = os.stat(path)
            now = time.time()
            
            con = self._get_connection()
            try:
                con.execute("INSERT OR REPLACE INTO files (path, mtime, size, indexed_at) VALUES (?, ?, ?, ?)",
                           [path, st.st_mtime, st.st_size, now])
                # No need for explicit commit in autocommit mode
            finally:
                con.close()
        except OSError as e:
            print(f"[warn] Could not stat file {path}: {e}", flush=True)
    
    def filter_unprocessed(self, uids_iter, batch_size=100000):
        """Return set of UIDs that are already processed (for filtering)"""
        processed = set()
        buf = []
        
        def run_batch(batch_uids):
            if not batch_uids:
                return
            
            con = self._get_connection()
            try:
                # Use IN clause for small batches, temp table for large ones
                if len(batch_uids) <= 100:
                    placeholders = ','.join('?' * len(batch_uids))
                    res = con.execute(f"""
                        SELECT uid FROM processed WHERE uid IN ({placeholders})
                    """, [str(u) for u in batch_uids]).fetchall()
                else:
                    con.execute("CREATE TEMP TABLE IF NOT EXISTS to_check(uid TEXT)")
                    con.execute("DELETE FROM to_check")
                    con.executemany("INSERT INTO to_check(uid) VALUES (?)", [(str(u),) for u in batch_uids])
                    res = con.execute("""
                        SELECT t.uid FROM to_check t JOIN processed p USING(uid)
                    """).fetchall()
                processed.update(u for (u,) in res)
            finally:
                con.close()
        
        for u in uids_iter:
            buf.append(u)
            if len(buf) >= batch_size:
                run_batch(buf)
                buf.clear()
        run_batch(buf)
        return processed
    
    def index_existing_chunks(self, out_prefix):
        """Index all existing chunk files under out_prefix tree"""
        base = os.path.basename(out_prefix)
        root_dir = os.path.dirname(out_prefix)
        files = []
        
        print(f"[ledger] Discovering existing chunk files...", flush=True)
        
        # Legacy files in root
        files += glob.glob(f"{out_prefix}.part*.chunk*.csv.gz")
        
        # Run_* subdirs
        if os.path.isdir(root_dir):
            for d in sorted(os.listdir(root_dir)):
                if d.startswith(base + ".Run_"):
                    files += glob.glob(os.path.join(root_dir, d, "*.chunk*.csv.gz"))
        
        print(f"[ledger] Found {len(files)} chunk files to check", flush=True)
        
        indexed_files = 0
        new_uids = 0
        
        for path in files:
            try:
                st = os.stat(path)
            except FileNotFoundError:
                continue
            
            # Check if already indexed and unchanged
            con = self._get_connection()
            try:
                cur = con.execute("SELECT mtime, size FROM files WHERE path = ?", [path]).fetchone()
            finally:
                con.close()
            if cur and (abs(cur[0] - st.st_mtime) < 1e-6) and (cur[1] == st.st_size):
                continue  # Already indexed and unchanged
            
            # Stream UIDs and insert in batches
            def stream_uids():
                try:
                    with gzip.open(path, "rt") as f:
                        next(f, None)  # Skip header
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            yield line.split('\t', 1)[0]
                except Exception as e:
                    print(f"[warn] Failed to read {path}: {e}", flush=True)
                    return
            
            batch = []
            con = self._get_connection()
            try:
                batch_count_before = con.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
            finally:
                con.close()
            
            for uid in stream_uids():
                batch.append(uid)
                if len(batch) >= 50000:
                    self.add_uids(batch)
                    batch.clear()
            if batch:
                self.add_uids(batch)
            
            con = self._get_connection()
            try:
                batch_count_after = con.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
            finally:
                con.close()
            new_uids += (batch_count_after - batch_count_before)
            self.record_file(path)
            indexed_files += 1
            
            if indexed_files % 10 == 0:
                print(f"[ledger] Indexed {indexed_files}/{len(files)} files, {new_uids:,} new UIDs so far...", flush=True)
        
        con = self._get_connection()
        try:
            total_uids = con.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
        finally:
            con.close()
        print(f"[ledger] Indexing complete: {indexed_files} files, {new_uids:,} new UIDs, {total_uids:,} total UIDs", flush=True)

def get_next_run_directory(out_prefix):
    """
    Determine the next run directory (Run_0, Run_1, etc.)
    Handles legacy chunks in main directory as Run_0 equivalent.
    Returns the path to the next run directory.
    """
    output_dir = os.path.dirname(out_prefix)
    base_name = os.path.basename(out_prefix)

    # Find existing run directories
    existing_runs = []
    has_legacy_chunks = False

    if os.path.exists(output_dir):
        for item in os.listdir(output_dir):
            if item.startswith(base_name + '.Run_'):
                try:
                    run_num = int(item.split('.Run_')[1])
                    existing_runs.append(run_num)
                except (ValueError, IndexError):
                    continue
            elif item.startswith(base_name + '.part') and item.endswith('.csv.gz'):
                # Found legacy chunks in main directory
                has_legacy_chunks = True

    # Determine next run number
    if existing_runs:
        next_run_num = max(existing_runs) + 1
    elif has_legacy_chunks:
        # Legacy chunks exist, so next run is Run_1
        next_run_num = 1
        print(f"[run] Found legacy chunks in main directory (Run_0 equivalent)", flush=True)
    else:
        next_run_num = 0

    run_dir_name = f"{base_name}.Run_{next_run_num}"
    run_dir_path = os.path.join(output_dir, run_dir_name)

    print(f"[run] Next run directory: {run_dir_name}", flush=True)
    print(f"[run] Full path: {run_dir_path}", flush=True)

    # Create the directory
    os.makedirs(run_dir_path, exist_ok=True)

    return run_dir_path



# Global variables for SIGTERM handler
# Global flag for SIGTERM handling
_terminate_requested = False

def _sigterm_handler(signum, frame):
    """Handle SIGTERM by setting termination flag"""
    global _terminate_requested
    _terminate_requested = True
    print(f"[sigterm] Termination requested, will finish current operations gracefully...", flush=True)

def main():
    ap = argparse.ArgumentParser(description="Compute cd_excl scores for a single region (US/EU/CN) with simple array-based sharding.")
    ap.add_argument('--cache-dir', required=True)
    ap.add_argument('--countries-parquet', required=True)
    ap.add_argument('--region-cache-dir', required=True)
    ap.add_argument('--out-prefix', required=True)
    ap.add_argument('--exclude-region', required=True, help="us | eu | cn")
    ap.add_argument('--years', type=int, default=5)

    ap.add_argument('--total-parts', type=int, default=1, help='total array tasks (array size)')
    ap.add_argument('--part-id', type=int, default=0, help='0..total-parts-1 array task ID')

    ap.add_argument('--log-every', type=int, default=200000)
    ap.add_argument('--chunk-size', type=int, default=340000, help='rows per gz chunk')

    # bounded work
    ap.add_argument('--num-chunks', type=int, default=None,
                    help='Process at most this many chunks then exit')

    # ingest options
    ap.add_argument('--flip-edges', action='store_true',
                    help='Input edges are (cited,citing); flip to (citing,cited) at ingest')
    args = ap.parse_args()

    # Resolve exclusion
    citer_filter = region_to_filter(args.exclude_region)
    print(f"[config] years={args.years}  filter={citer_filter}  flip_edges={args.flip_edges}", flush=True)

    vpath = os.path.join(args.cache_dir, 'paper_years.parquet')
    epath = os.path.join(args.cache_dir, 'edges.parquet')

    t0 = time.perf_counter()

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

    # 5) Shard rows (keep original deterministic order)
    n = vt.num_rows
    global_total_parts = args.total_parts
    global_part_id = args.part_id
    start, end = shard_slice(n, global_part_id, global_total_parts)
    print(f"[shard] global_part_id={global_part_id} total_parts={global_total_parts}  rows=[{start}:{end})  count={end-start}", flush=True)
    print(f"[plan] Note: hash partitioning overrides legacy shard slice; start/end shown only for reference", flush=True)

    # Get total papers from the vertex table before cleanup
    total_papers = vt.num_rows
    print(f"[run] Total papers in dataset: {total_papers:,}", flush=True)
    
    # Clear UID map and release vertex table memory early
    g.clear_uid_map()
    del vt

    # Initialize SQLite ledger for UID tracking
    resume_dir = os.path.join(os.path.dirname(args.out_prefix), ".resume")
    os.makedirs(resume_dir, exist_ok=True)
    db_path = os.path.join(resume_dir, "uid_ledger.sqlite")
    
    # Optional: use SLURM job-specific temp directory
    tmpdir = os.getenv("SQLITE_TMPDIR")
    if not tmpdir:
        job_id = os.getenv("SLURM_JOB_ID", "local")
        tmpdir = f"/tmp/sqlite_tmp_{job_id}"
        os.makedirs(tmpdir, exist_ok=True)
    
    print(f"[ledger] Initializing SQLite ledger: {db_path}", flush=True)
    ledger = UidLedgerSQLite(db_path, tmpdir=tmpdir, part_id=global_part_id)
    
    # Index all existing chunks with race condition guard using marker file
    marker = os.path.join(resume_dir, "index.done")
    if global_part_id == 0:
        ledger.index_existing_chunks(args.out_prefix)
        Path(marker).touch()
        print(f"[ledger] Created index completion marker: {marker}", flush=True)
    else:
        if os.path.exists(marker):
            print("[ledger] Index already done by another part", flush=True)
        else:
            # Lightweight guard against indexing race - fallback for first-run edge cases
            legacy = glob.glob(f"{args.out_prefix}.part*.chunk*.csv.gz")
            con = ledger._get_connection()
            try:
                cnt = con.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
            finally:
                con.close()
            if legacy and cnt == 0:
                print("[ledger] processed empty but legacy chunks exist; running one-time index here", flush=True)
                ledger.index_existing_chunks(args.out_prefix)
                Path(marker).touch()
                print(f"[ledger] Created index completion marker: {marker}", flush=True)
            else:
                print("[ledger] Skipping index_existing_chunks(); handled by another part", flush=True)
    
    # Check if all papers are processed
    con = ledger._get_connection()
    try:
        total_processed_uids = con.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
    finally:
        con.close()
    if total_processed_uids >= total_papers:
        print(f"[run] All papers already processed across all runs!", flush=True)
        return

    print(f"[run] Papers already processed: {total_processed_uids:,}", flush=True)
    print(f"[run] Papers remaining: {total_papers - total_processed_uids:,}", flush=True)

    # Create new run directory for fresh work (only if needed)
    if total_processed_uids > 0:
        # We have previous work, need new run directory
        run_dir = get_next_run_directory(args.out_prefix)
        actual_out_prefix = os.path.join(run_dir, "scores_excl")
        print(f"[run] Using new run directory: {run_dir}", flush=True)
    else:
        # No previous work, use original prefix
        actual_out_prefix = args.out_prefix
        print(f"[run] No previous work found, using original prefix", flush=True)

    # Update args to use the output prefix
    args.out_prefix = actual_out_prefix

    # Use fully streaming compute with hash partitioning (no large Python lists)
    print(f"[compute] Starting fully streaming computation for part {global_part_id}/{global_total_parts}...", flush=True)
    t_start_loop = time.perf_counter()
    
    # Stream from the same source used for vertices (paper_years.parquet)
    vpath = args.cache_dir + "/paper_years.parquet"
    print(f"[compute] Streaming from {vpath}...", flush=True)
    
    try:
        import xxhash
        def part_of(uid_str): 
            return xxhash.xxh64(uid_str).intdigest() % global_total_parts
        print(f"[compute] Using xxhash for stable partitioning", flush=True)
    except ImportError:
        import zlib
        def part_of(uid_str): 
            return zlib.adler32(uid_str.encode()) % global_total_parts
        print(f"[compute] Using zlib.adler32 for stable partitioning", flush=True)
    
    # Initialize streaming compute state
    chunk_idx = 0
    chunks_done = 0
    max_chunks_to_do = args.num_chunks  # may be None
    out_rows = []
    processed_local = 0
    bad = 0
    years = args.years
    filt_enum = citer_filter  # Map chosen region to filter enum
    
    # Register SIGTERM and SIGINT handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)
    
    # Stream dataset in batches and compute immediately
    dataset = ds.dataset(vpath, format="parquet")
    scanner = dataset.scanner(columns=['UID','paper_id'], batch_size=1<<20, use_threads=True)
    
    print(f"[compute] Part {global_part_id} starting streaming computation", flush=True)
    
    for batch_i, batch in enumerate(scanner.to_batches(), 1):
        # SIGTERM check
        if _terminate_requested:
            print(f"[sigterm] Termination requested during batch {batch_i}, writing partial chunk...", flush=True)
            if out_rows:
                write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
                chunk_path = f"{args.out_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
                print(f"[sigterm] Wrote partial chunk: {chunk_path}", flush=True)
                
                uids_written = [u for (u, _) in out_rows]
                ledger.add_uids(uids_written)
                ledger.record_file(chunk_path)
                chunks_done += 1
                print(f"[sigterm] Updated ledger with {len(uids_written)} UIDs", flush=True)
            
            print(f"[sigterm] Graceful shutdown complete", flush=True)
            sys.exit(0)
        
        uids_batch = [str(u) for u in batch.column('UID').to_pylist()]
        pids_batch = batch.column('paper_id').to_pylist()
        
        # Route to this part via stable hash
        part_uids, part_pids = [], []
        for u, p in zip(uids_batch, pids_batch):
            if part_of(u) == global_part_id:
                part_uids.append(u)
                part_pids.append(p)
        
        if part_uids:
            # Anti-join against ledger
            processed_set = set(ledger._filter_unprocessed_batch(part_uids))
            
            # Compute immediately for unprocessed papers
            for u, p in zip(part_uids, part_pids):
                if u in processed_set:
                    continue  # Already processed
                
                # Check for termination before each compute (expensive operation)
                if _terminate_requested:
                    print(f"[sigterm] Termination requested during compute, writing partial chunk...", flush=True)
                    if out_rows:
                        write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
                        chunk_path = f"{args.out_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
                        uids_written = [uid for (uid, _) in out_rows]
                        ledger.add_uids(uids_written)
                        ledger.record_file(chunk_path)
                        chunks_done += 1
                        print(f"[sigterm] Updated ledger with {len(uids_written)} UIDs", flush=True)
                    print(f"[sigterm] Graceful shutdown complete", flush=True)
                    sys.exit(0)
                
                try:
                    val = g.cdindex_filtered(p, years, filt_enum)
                except Exception as e:
                    bad += 1
                    if bad <= 5:
                        print("[warn] cdindex_filtered failed for", p, repr(e), flush=True)
                    if bad > 1000:
                        print("[fatal] too many per-paper failures; aborting", flush=True)
                        sys.exit(2)
                    val = float('nan')
                
                out_rows.append((u, val))
                processed_local += 1
                
                # Flush chunk when full
                if len(out_rows) >= args.chunk_size:
                    write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
                    chunk_path = f"{args.out_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
                    print(f"wrote chunk: {chunk_path}", flush=True)
                    
                    uids_written = [uid for (uid, _) in out_rows]
                    ledger.add_uids(uids_written)
                    ledger.record_file(chunk_path)
                    
                    out_rows.clear()
                    chunk_idx += 1
                    chunks_done += 1
                    
                    # Check if we've completed assigned chunks
                    if max_chunks_to_do is not None and chunks_done >= max_chunks_to_do:
                        print(f"[plan] Completed assigned {chunks_done} chunks for part {global_part_id}. Exiting.", flush=True)
                        return
        
        # Progress logging
        if processed_local > 0 and (processed_local % args.log_every == 0):
            elapsed = time.perf_counter() - t_start_loop
            rate = processed_local / max(1e-9, elapsed)
            print(f"[part {global_part_id}/{global_total_parts}] processed {processed_local:,}  ({rate:,.1f} papers/sec)", flush=True)
    
    # Final flush
    if out_rows:
        write_chunk(args.out_prefix, global_part_id, chunk_idx, out_rows)
        chunk_path = f"{args.out_prefix}.part{global_part_id:04d}.chunk{chunk_idx:05d}.csv.gz"
        print(f"wrote final chunk: {chunk_path}", flush=True)
        
        uids_written = [uid for (uid, _) in out_rows]
        ledger.add_uids(uids_written)
        ledger.record_file(chunk_path)
        chunks_done += 1
    
    if processed_local == 0:
        print(f"[run] No remaining papers for part {global_part_id} to process!", flush=True)
        return

    elapsed = time.perf_counter() - t0
    print(f"Total time: {elapsed:,.1f} sec", flush=True)
    
    print(f"[ledger] Part {global_part_id} completed successfully", flush=True)

if __name__ == '__main__':
    main()
