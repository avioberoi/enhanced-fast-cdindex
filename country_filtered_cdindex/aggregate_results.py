#!/usr/bin/env python3
"""
Aggregate Filtered CD-Index Results

This script combines results from distributed array jobs into a single dataset.
"""

import os
import pandas as pd
import glob
import argparse
import logging

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

def aggregate_results(results_dir: str, output_file: str):
    """Aggregate array job results into single file"""
    logger = setup_logging()
    
    # Find all result files
    pattern = os.path.join(results_dir, "filtered_cdindex_part_*.csv.gz")
    result_files = glob.glob(pattern)
    
    if not result_files:
        raise FileNotFoundError(f"No result files found in {results_dir}")
    
    logger.info(f"Found {len(result_files)} result files")
    
    # Load and combine all results
    all_results = []
    total_papers = 0
    
    for i, file_path in enumerate(sorted(result_files)):
        logger.info(f"Loading {file_path} ({i+1}/{len(result_files)})")
        
        try:
            df = pd.read_csv(file_path, compression='gzip')
            all_results.append(df)
            total_papers += len(df)
            logger.info(f"  Loaded {len(df):,} papers")
            
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            continue
    
    if not all_results:
        raise RuntimeError("No valid result files found")
    
    # Combine all dataframes
    logger.info(f"Combining {len(all_results)} dataframes...")
    combined_df = pd.concat(all_results, ignore_index=True)
    
    # Remove duplicates (shouldn't happen but just in case)
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['paper_id'])
    final_count = len(combined_df)
    
    if initial_count != final_count:
        logger.warning(f"Removed {initial_count - final_count} duplicate papers")
    
    # Save combined results
    logger.info(f"Saving {final_count:,} papers to {output_file}")
    
    if output_file.endswith('.gz'):
        combined_df.to_csv(output_file, index=False, compression='gzip')
    else:
        combined_df.to_csv(output_file, index=False)
    
    # Statistics
    logger.info("=== Aggregation Summary ===")
    logger.info(f"Total papers: {final_count:,}")
    logger.info(f"Columns: {list(combined_df.columns)}")
    
    # Count non-NaN values for each filter
    for col in combined_df.columns:
        if col.startswith('cdindex_'):
            valid_count = combined_df[col].notna().sum()
            logger.info(f"  {col}: {valid_count:,} valid scores ({valid_count/final_count*100:.1f}%)")
    
    logger.info(f"Combined results saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Aggregate Filtered CD-Index Results')
    parser.add_argument('--results-dir', required=True,
                       help='Directory containing array job results')
    parser.add_argument('--output-file', required=True,
                       help='Output file for combined results')
    
    args = parser.parse_args()
    
    aggregate_results(args.results_dir, args.output_file)

if __name__ == "__main__":
    main()
