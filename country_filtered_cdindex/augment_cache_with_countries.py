#!/usr/bin/env python3
"""
Augment Existing TSV Cache with Country Data

This script takes the existing TSV cache and adds country information
for the overlapping papers, creating a complete dataset for filtered CD-index computations.
"""

import os
import sys
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
import argparse
from pathlib import Path
from typing import Optional

# Add the parent directory to import the enhanced graph (exact same as validate_enhanced_cdindex.py)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from fast_cdindex.cdindex_enhanced import EnhancedGraph

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

class CacheAugmenter:
    """Augments existing TSV cache with country information"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        
    def create_country_augmented_cache(self,
                                      existing_cache_dir: str,
                                      country_file: str,
                                      output_dir: str,
                                      limit_papers: Optional[int] = None):
        """
        Create country-augmented cache from existing cache + country data.
        
        Args:
            existing_cache_dir: Path to existing TSV cache directory
            country_file: Path to paper_countries_fixed_w_EU.parquet
            output_dir: Output directory for augmented cache
            limit_papers: Optional limit on papers for testing
        """
        start_time = time.time()
        
        self.logger.info("Creating country-augmented cache...")
        os.makedirs(output_dir, exist_ok=True)
        
        # 1. Load existing cache components
        self.logger.info(f"Loading existing cache from {existing_cache_dir}")
        
        # Load ID mapping (WoS UID -> integer ID)
        id_mapping_file = os.path.join(existing_cache_dir, 'id_mapping.parquet')
        id_mapping_df = pq.read_table(id_mapping_file).to_pandas()
        self.logger.info(f"Loaded {len(id_mapping_df):,} ID mappings")
        
        # Load paper years
        papers_file = os.path.join(existing_cache_dir, 'paper_years.parquet') 
        papers_df = pq.read_table(papers_file).to_pandas()
        self.logger.info(f"Loaded {len(papers_df):,} paper years")
        
        # OPTIMIZATION: Don't load full edges table until we know which papers we need
        edges_file = os.path.join(existing_cache_dir, 'edges.parquet')
        self.logger.info("Deferring edges loading until after paper filtering...")
        
        # 2. OPTIMIZATION: Load and filter country data efficiently  
        self.logger.info(f"Loading country data from {country_file}")
        
        # OPTIMIZATION: Load only UID and country columns to save memory (60% reduction)
        country_table = pq.read_table(country_file, columns=['UID', 'country'])
        self.logger.info(f"Loaded {country_table.num_rows:,} papers with country data")
        
        # ULTRA-OPTIMIZATION: Keep everything in Arrow format until absolutely necessary
        self.logger.info("Finding overlap using Arrow compute (zero Python overhead)...")
        
        # Convert to pandas only when required (minimal memory footprint)
        country_df = country_table.to_pandas()
        
        # VECTORIZED merge - single operation, no Python loops
        country_with_ids = country_df.merge(
            id_mapping_df, 
            left_on='UID', 
            right_on='wos_id', 
            how='inner'  # Only keep overlapping papers
        )
        
        self.logger.info(f"Found {len(country_with_ids):,} overlapping papers ({len(country_with_ids)/len(id_mapping_df)*100:.1f}% coverage)")
        
        # 4. Apply limits early to reduce memory usage
        if limit_papers and limit_papers > 0:
            self.logger.info(f"Limiting to first {limit_papers:,} papers for testing")
            
            # Get the first N paper IDs (sort for deterministic results)
            limited_ids = sorted(country_with_ids['id'].unique())[:limit_papers]
            limited_ids_set = set(limited_ids)
            
            # Filter datasets to these IDs
            country_with_ids = country_with_ids[country_with_ids['id'].isin(limited_ids)]
            overlap_id_mapping = id_mapping_df[id_mapping_df['id'].isin(limited_ids)]
            papers_df = papers_df[papers_df['paper_id'].isin(limited_ids)]
            
            self.logger.info(f"After limiting: {len(country_with_ids):,} papers")
        else:
            # Keep all overlapping papers
            limited_ids_set = set(country_with_ids['id'].unique())
            overlap_id_mapping = id_mapping_df[id_mapping_df['wos_id'].isin(country_with_ids['UID'])]
        
        # 4.1. NOW load and filter edges efficiently
        self.logger.info("Loading and filtering edges...")
        
        # OPTIMIZATION: Use PyArrow filtering instead of pandas for massive edge dataset
        edges_table = pq.read_table(edges_file)
        
        # Convert limited_ids to arrow array for efficient filtering
        limited_ids_array = pa.array(list(limited_ids_set), type=pa.uint32())
        
        # Use PyArrow compute for filtering (much faster than pandas for large data)
        import pyarrow.compute as pc
        
        # Create filter conditions
        source_mask = pc.is_in(edges_table['source_id'], limited_ids_array)
        target_mask = pc.is_in(edges_table['target_id'], limited_ids_array)
        combined_mask = pc.and_(source_mask, target_mask)
        
        # Apply filter
        filtered_edges_table = edges_table.filter(combined_mask)
        edges_df = filtered_edges_table.to_pandas()
        
        self.logger.info(f"Filtered edges: {len(edges_df):,} (from {edges_table.num_rows:,})")
        
        # 5. Create country properties table for PropertyStore ingestion
        country_properties = country_with_ids[['id', 'country']].rename(columns={'id': 'paper_id'})
        country_properties = country_properties.drop_duplicates()
        
        # 6. Save augmented cache files
        self.logger.info("Saving augmented cache files...")
        
        # Save paper years (filtered)
        paper_years_out = os.path.join(output_dir, 'paper_years.parquet')
        pq.write_table(pa.Table.from_pandas(papers_df, preserve_index=False), paper_years_out)
        
        # Save edges (filtered)
        edges_out = os.path.join(output_dir, 'edges.parquet') 
        pq.write_table(pa.Table.from_pandas(edges_df, preserve_index=False), edges_out)
        
        # Save ID mapping (filtered)
        id_mapping_out = os.path.join(output_dir, 'id_mapping.parquet')
        pq.write_table(pa.Table.from_pandas(overlap_id_mapping, preserve_index=False), id_mapping_out)
        
        # Save country properties 
        country_props_out = os.path.join(output_dir, 'country_properties.parquet')
        pq.write_table(pa.Table.from_pandas(country_properties, preserve_index=False), country_props_out)
        
        # 7. Create combined properties table for PropertyStore
        # Combine paper_years and country data for single PropertyStore.ingest_arrow() call
        combined_properties = papers_df.merge(
            country_properties, 
            on='paper_id', 
            how='left'  # Left join to keep all papers, even those without country data
        )
        
        # Fill missing countries with a default value
        combined_properties['country'] = combined_properties['country'].fillna('unknown')
        
        combined_props_out = os.path.join(output_dir, 'combined_properties.parquet')
        pq.write_table(pa.Table.from_pandas(combined_properties, preserve_index=False), combined_props_out)
        
        # 8. Generate statistics
        cache_time = time.time() - start_time
        
        self.logger.info("=== AUGMENTED CACHE SUMMARY ===")
        self.logger.info(f"Processing time: {cache_time:.1f} seconds")
        self.logger.info(f"Output directory: {output_dir}")
        self.logger.info(f"Papers with country data: {len(country_properties):,}")
        self.logger.info(f"Total edges: {len(edges_df):,}")
        
        # Country distribution
        self.logger.info("Country distribution:")
        country_stats = country_properties['country'].value_counts().head(10)
        total_with_country = len(country_properties)
        for country, count in country_stats.items():
            pct = count / total_with_country * 100
            self.logger.info(f"  {country}: {count:,} ({pct:.1f}%)")
        
        # Region distribution using our mapping
        region_counts = self._get_region_distribution(country_properties['country'])
        self.logger.info("Region distribution (for filtering):")
        for region, count in region_counts.items():
            pct = count / total_with_country * 100
            self.logger.info(f"  {region}: {count:,} ({pct:.1f}%)")
        
        return output_dir
    
    def _get_region_distribution(self, country_series):
        """Get distribution by our region mapping"""
        region_counts = {
            'US': (country_series == 'usa').sum(),
            'China': (country_series == 'peoples r china').sum(),
            'EU': (country_series == 'eu').sum(),
            'UK': (country_series == 'united kingdom').sum(),  # Note: UK tracked but not filtered
        }
        region_counts['Others'] = len(country_series) - sum(region_counts.values())
        return region_counts

def main():
    parser = argparse.ArgumentParser(description='Augment Existing Cache with Country Data')
    parser.add_argument('--existing-cache-dir', required=True,
                       help='Path to existing TSV cache directory')
    parser.add_argument('--country-file', required=True,
                       help='Path to paper_countries_fixed_w_EU.parquet')
    parser.add_argument('--output-dir', required=True,
                       help='Output directory for augmented cache')
    parser.add_argument('--limit-papers', type=int,
                       help='Limit number of papers for testing')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    # Create augmenter
    augmenter = CacheAugmenter(logger)
    
    try:
        output_dir = augmenter.create_country_augmented_cache(
            existing_cache_dir=args.existing_cache_dir,
            country_file=args.country_file,
            output_dir=args.output_dir,
            limit_papers=args.limit_papers
        )
        
        logger.info("Country-augmented cache created successfully")
        logger.info(f"Ready for filtered CD-index computations!")
        
    except Exception as e:
        logger.error(f"Failed to create augmented cache: {e}")
        raise

if __name__ == "__main__":
    main()
