#!/usr/bin/env python3
"""
Discovery Script: Scan existing output files to build consolidated UID sets.

This script finds all UIDs that have already been processed across multiple
runs/directories and prepares the resume state for a new run.

Usage:
    python discover_processed_uids.py --search-dirs /path/to/scores1 /path/to/scores2 \
                                      --out-dir /path/to/new_run \
                                      --total-parts 5 \
                                      --exclude-region us
"""

import argparse
import os
import gzip
import glob
import pickle
from pathlib import Path
from collections import defaultdict
import time

# No hash partitioning needed - using simple sharding approach

def discover_uids_from_file(filepath):
    """Extract UIDs from a single output file."""
    uids = set()
    try:
        if filepath.endswith('.gz'):
            opener = gzip.open
        else:
            opener = open
            
        with opener(filepath, 'rt') as f:
            # Skip header
            next(f, None)
            
            for line_num, line in enumerate(f, 2):  # Start from line 2 after header
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 1:
                    uid = parts[0].strip()
                    if uid and uid != 'UID':  # Skip any stray headers
                        uids.add(uid)
                        
    except Exception as e:
        print(f"[warn] Error reading {filepath}: {e}")
        
    return uids

def discover_processed_uids(search_dirs, exclude_region):
    """
    Memory-efficient discovery of processed UIDs from existing output files.
    
    Returns:
        set: All UIDs that have been processed across all runs
    """
    all_uids = set()
    total_files = 0
    
    print(f"[discovery] Scanning directories for {exclude_region} exclusion scores...")
    
    # Single pattern list to avoid redundant globbing
    patterns = [
        f"**/scores_excl*.part*.chunk*.csv.gz",  # Current format
        f"**/*{exclude_region}*/scores_excl*.part*.chunk*.csv.gz",  # Region subdirs
        f"**/scores_excl*.chunk*.csv.gz",  # Legacy format
        f"**/*excl*.csv.gz",  # Catch-all
    ]
    
    for search_dir in search_dirs:
        search_path = Path(search_dir)
        if not search_path.exists():
            print(f"[warn] Directory does not exist: {search_dir}")
            continue
            
        print(f"[discovery] Scanning: {search_dir}")
        
        # Collect all matching files first to avoid duplicate processing
        all_files = set()
        for pattern in patterns:
            all_files.update(search_path.glob(pattern))
        
        dir_uids_count = 0
        for filepath in all_files:
            file_uids = discover_uids_from_file(str(filepath))
            all_uids.update(file_uids)
            total_files += 1
            dir_uids_count += len(file_uids)
            
            if len(file_uids) > 0:
                print(f"[discovery] Found {len(file_uids):,} UIDs in {filepath.name}")
        
        print(f"[discovery] Directory {search_dir}: {len(all_files)} files, {dir_uids_count:,} UIDs")
    
    print(f"[discovery] Total: {total_files} files scanned, {len(all_uids):,} unique UIDs found")
    return all_uids

def create_global_processed_file(all_uids, out_dir):
    """
    Create a single global file containing all processed UIDs.
    This will be loaded by all array jobs to filter out already-processed work.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    processed_file = out_path / "global_processed_uids.pkl"
    
    # Save all processed UIDs in a single file
    tmp_file = str(processed_file) + ".tmp"
    with open(tmp_file, 'wb') as f:
        pickle.dump(all_uids, f)
    os.replace(tmp_file, processed_file)
    
    print(f"[global] Created global processed file with {len(all_uids):,} UIDs")
    print(f"[global] File: {processed_file}")
    
    return processed_file

def create_discovery_summary(all_uids, out_dir, search_dirs, exclude_region):
    """Create a summary file documenting the discovery results."""
    summary_file = Path(out_dir) / "discovery_summary.txt"
    
    with open(summary_file, 'w') as f:
        f.write(f"Discovery Summary\n")
        f.write(f"================\n\n")
        f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Exclude Region: {exclude_region}\n")
        f.write(f"Search Directories: {', '.join(search_dirs)}\n")
        f.write(f"Output Directory: {out_dir}\n\n")
        
        f.write(f"Results:\n")
        f.write(f"- Total UIDs discovered: {len(all_uids):,}\n")
        f.write(f"- Global processed file: global_processed_uids.pkl\n\n")
        
        f.write(f"Execution Strategy:\n")
        f.write(f"- Array jobs will use simple sharding (not hash partitioning)\n")
        f.write(f"- Each job processes its shard minus the globally processed UIDs\n")
        f.write(f"- No real-time UID tracking during execution\n")

def main():
    parser = argparse.ArgumentParser(description="Discover processed UIDs from existing output files")
    parser.add_argument("--search-dirs", nargs='+', required=True, 
                       help="Directories to search for existing output files")
    parser.add_argument("--out-dir", required=True,
                       help="Output directory for new run (where resume files will be created)")
# Removed unused total-parts argument
    parser.add_argument("--exclude-region", required=True, choices=["us", "eu", "cn"],
                       help="Region being excluded (for file pattern matching)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be discovered without creating resume files")
    
    args = parser.parse_args()
    
    print(f"[config] Discovering UIDs for {args.exclude_region} exclusion")
    print(f"[config] Search directories: {args.search_dirs}")
    print(f"[config] Output directory: {args.out_dir}")
    print(f"[config] Dry run: {args.dry_run}")
    print()
    
    # Phase 1: Discover all processed UIDs
    start_time = time.time()
    all_uids = discover_processed_uids(args.search_dirs, args.exclude_region)
    discovery_time = time.time() - start_time
    
    print(f"\n[discovery] Completed in {discovery_time:.1f}s")
    
    if len(all_uids) == 0:
        print("[warn] No UIDs discovered! Check search directories and file patterns.")
        return
    
    if args.dry_run:
        print(f"\n[dry-run] Would create global processed file in: {args.out_dir}")
        print(f"[dry-run] Total UIDs to skip in new run: {len(all_uids):,}")
        print(f"[dry-run] Array jobs will use simple sharding to divide remaining work")
        return
    
    # Phase 2: Create global processed file
    print(f"\n[global] Creating global processed file in {args.out_dir}...")
    create_global_processed_file(all_uids, args.out_dir)
    
    # Phase 3: Create summary
    create_discovery_summary(all_uids, args.out_dir, args.search_dirs, args.exclude_region)
    
    print(f"\n[success] Discovery complete!")
    print(f"[success] Global processed file created: global_processed_uids.pkl")
    print(f"[success] {len(all_uids):,} UIDs will be skipped in new run")
    print(f"[success] Summary written to: {args.out_dir}/discovery_summary.txt")

if __name__ == "__main__":
    main()
