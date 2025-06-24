"""
Data Loading and Preparation Module

Handles loading, caching, and preparation of benchmark data including:
- WoS dataset loading and validation
- Micro benchmark data generation
- Data caching and retrieval
- Arrow table management
"""

import os
import logging
import time
from typing import Dict, Any, Optional, Tuple, List
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path

# Add the parent directory to import the enhanced graph
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from fast_cdindex.cdindex_enhanced import EnhancedGraph

from config import BenchmarkConfig


class DataLoader:
    """
    Handles all data loading operations for benchmarking.
    
    Provides methods for:
    - Loading raw WoS data
    - Creating and managing micro benchmark datasets
    - Caching data for repeated use
    - Validating data integrity
    """
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Ensure cache directory exists
        os.makedirs(self.config.data.cache_dir, exist_ok=True)
    
    def load_wos_data(self, use_cache: bool = True) -> Tuple[pa.Table, pa.Table]:
        """
        Load the full WoS dataset.
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            Tuple of (vertices_table, edges_table)
        """
        cache_vertices = os.path.join(self.config.data.cache_dir, "full_vertices.parquet")
        cache_edges = os.path.join(self.config.data.cache_dir, "full_edges.parquet")
        
        # Try to load from cache first
        if use_cache and os.path.exists(cache_vertices) and os.path.exists(cache_edges):
            self.logger.info("Loading WoS data from cache...")
            vertices_table = pq.read_table(cache_vertices)
            edges_table = pq.read_table(cache_edges)
            self.logger.info(f"Loaded from cache: {vertices_table.num_rows:,} vertices, {edges_table.num_rows:,} edges")
            return vertices_table, edges_table
        
        # Load from raw files
        self.logger.info("Loading WoS data from raw files...")
        vertices_path = os.path.join(self.config.data.wos_data_dir, self.config.data.vertices_file)
        edges_path = os.path.join(self.config.data.wos_data_dir, self.config.data.edges_file)
        
        # Load vertices
        self.logger.info(f"Loading vertices from {vertices_path}")
        vertices_df = pd.read_csv(vertices_path, sep='\t', dtype={'paper_id': 'uint32', 'year': 'uint16'})
        vertices_table = pa.Table.from_pandas(vertices_df, preserve_index=False)
        
        # Load edges
        self.logger.info(f"Loading edges from {edges_path}")
        edges_df = pd.read_csv(edges_path, sep='\t', dtype={'source_id': 'uint32', 'target_id': 'uint32'})
        edges_table = pa.Table.from_pandas(edges_df, preserve_index=False)
        
        self.logger.info(f"Loaded: {vertices_table.num_rows:,} vertices, {edges_table.num_rows:,} edges")
        
        # Cache the data
        if use_cache:
            self.logger.info("Caching WoS data...")
            pq.write_table(vertices_table, cache_vertices)
            pq.write_table(edges_table, cache_edges)
        
        return vertices_table, edges_table
    
    def create_micro_dataset(self, 
                           vertices_table: pa.Table, 
                           edges_table: pa.Table,
                           force_recreate: bool = False) -> Tuple[pa.Table, pa.Table, pa.Array]:
        """
        Create a micro benchmark dataset by sampling from the full data.
        
        Args:
            vertices_table: Full vertices table
            edges_table: Full edges table  
            force_recreate: Force recreation even if cache exists
            
        Returns:
            Tuple of (micro_vertices, micro_edges, benchmark_ids)
        """
        cache_vertices = os.path.join(self.config.data.cache_dir, "micro_vertices.parquet")
        cache_edges = os.path.join(self.config.data.cache_dir, "micro_edges.parquet")
        cache_ids = os.path.join(self.config.data.cache_dir, "micro_ids.parquet")
        
        # Check if cached micro data exists
        if not force_recreate and all(os.path.exists(p) for p in [cache_vertices, cache_edges, cache_ids]):
            self.logger.info("Loading micro dataset from cache...")
            micro_vertices = pq.read_table(cache_vertices)
            micro_edges = pq.read_table(cache_edges)
            benchmark_ids_table = pq.read_table(cache_ids)
            benchmark_ids = benchmark_ids_table.column("paper_id").combine_chunks()
            
            self.logger.info(f"Loaded micro dataset: {micro_vertices.num_rows:,} vertices, "
                           f"{micro_edges.num_rows:,} edges, {len(benchmark_ids):,} benchmark IDs")
            return micro_vertices, micro_edges, benchmark_ids
        
        self.logger.info(f"Creating micro dataset with {self.config.data.micro_vertices:,} vertices...")
        
        # Sample vertices (take first N for reproducibility)
        sample_size = min(self.config.data.micro_vertices, vertices_table.num_rows)
        micro_vertices = vertices_table.slice(0, sample_size)
        
        # Get sampled vertex IDs
        sampled_ids = set(micro_vertices.column("paper_id").to_pylist())
        self.logger.info(f"Sampled {len(sampled_ids):,} vertex IDs")
        
        # Filter edges to only include those between sampled vertices
        self.logger.info("Filtering edges...")
        edges_pandas = edges_table.to_pandas()
        mask = edges_pandas['source_id'].isin(sampled_ids) & edges_pandas['target_id'].isin(sampled_ids)
        filtered_edges = edges_pandas[mask]
        micro_edges = pa.Table.from_pandas(filtered_edges, preserve_index=False)
        
        self.logger.info(f"Filtered edges: {micro_edges.num_rows:,} edges remain")
        
        # Create benchmark IDs
        benchmark_size = min(self.config.data.micro_benchmark_papers, micro_vertices.num_rows)
        benchmark_ids = micro_vertices.column("paper_id").slice(0, benchmark_size)
        benchmark_ids_table = pa.Table.from_arrays([benchmark_ids], names=["paper_id"])
        
        # Cache the micro dataset
        self.logger.info("Caching micro dataset...")
        pq.write_table(micro_vertices, cache_vertices)
        pq.write_table(micro_edges, cache_edges)
        pq.write_table(benchmark_ids_table, cache_ids)
        
        self.logger.info(f"Created micro dataset: {micro_vertices.num_rows:,} vertices, "
                        f"{micro_edges.num_rows:,} edges, {len(benchmark_ids):,} benchmark IDs")
        
        return micro_vertices, micro_edges, benchmark_ids
    
    def load_micro_dataset(self) -> Dict[str, Any]:
        """
        Load micro benchmark dataset and create an EnhancedGraph.
        
        Returns:
            Dictionary containing graph, benchmark_ids, and metadata
        """
        cache_vertices = os.path.join(self.config.data.cache_dir, "micro_vertices.parquet")
        cache_edges = os.path.join(self.config.data.cache_dir, "micro_edges.parquet")
        cache_ids = os.path.join(self.config.data.cache_dir, "micro_ids.parquet")
        
        # Check if micro data exists
        missing_files = []
        for path, name in [(cache_vertices, "micro_vertices.parquet"), 
                          (cache_edges, "micro_edges.parquet"),
                          (cache_ids, "micro_ids.parquet")]:
            if not os.path.exists(path):
                missing_files.append(name)
        
        if missing_files:
            self.logger.info(f"Micro data files missing: {missing_files}. Creating micro dataset...")
            # Load full data and create micro dataset
            vertices_table, edges_table = self.load_wos_data()
            micro_vertices, micro_edges, benchmark_ids = self.create_micro_dataset(vertices_table, edges_table)
        else:
            # Load existing micro data
            self.logger.info("Loading existing micro dataset...")
            micro_vertices = pq.read_table(cache_vertices)
            micro_edges = pq.read_table(cache_edges)
            benchmark_ids_table = pq.read_table(cache_ids)
            benchmark_ids = benchmark_ids_table.column("paper_id").combine_chunks()
        
        # Create and populate EnhancedGraph
        self.logger.info("Creating EnhancedGraph...")
        graph = EnhancedGraph()
        
        # Ingest vertices in chunks
        chunk_size = self.config.performance.chunk_size
        self.logger.info("Ingesting vertices...")
        vertex_count = 0
        for batch in micro_vertices.to_batches(max_chunksize=chunk_size):
            batch_table = pa.Table.from_batches([batch])
            graph.add_vertex_batch(batch_table)
            vertex_count += batch.num_rows
        
        # Ingest edges in chunks
        self.logger.info("Ingesting edges...")
        edge_count = 0
        for batch in micro_edges.to_batches(max_chunksize=chunk_size):
            batch_table = pa.Table.from_batches([batch])
            graph.add_edge_batch(batch_table)
            edge_count += batch.num_rows
        
        # Ingest properties for filtering
        self.logger.info("Ingesting properties and building indexes...")
        graph.ingest_properties(micro_vertices)
        graph.build_property_indexes()
        
        self.logger.info(f"EnhancedGraph ready: {vertex_count:,} vertices, {edge_count:,} edges")
        
        return {
            'graph': graph,
            'benchmark_ids': benchmark_ids,
            'vertex_count': vertex_count,
            'edge_count': edge_count,
            'vertices_table': micro_vertices,
            'edges_table': micro_edges
        }
    
    def validate_data(self, vertices_table: pa.Table, edges_table: pa.Table) -> List[str]:
        """
        Validate data integrity and return list of issues found.
        
        Args:
            vertices_table: Vertices table to validate
            edges_table: Edges table to validate
            
        Returns:
            List of validation issues (empty if valid)
        """
        issues = []
        
        # Check for required columns
        vertex_columns = vertices_table.column_names
        edge_columns = edges_table.column_names
        
        if 'paper_id' not in vertex_columns:
            issues.append("Missing 'paper_id' column in vertices")
        if 'year' not in vertex_columns:
            issues.append("Missing 'year' column in vertices")
        if 'source_id' not in edge_columns:
            issues.append("Missing 'source_id' column in edges")
        if 'target_id' not in edge_columns:
            issues.append("Missing 'target_id' column in edges")
        
        # Check for nulls
        for col_name in vertex_columns:
            null_count = pa.compute.sum(pa.compute.is_null(vertices_table.column(col_name))).as_py()
            if null_count > 0:
                issues.append(f"Found {null_count} null values in vertices.{col_name}")
        
        for col_name in edge_columns:
            null_count = pa.compute.sum(pa.compute.is_null(edges_table.column(col_name))).as_py()
            if null_count > 0:
                issues.append(f"Found {null_count} null values in edges.{col_name}")
        
        # Check for valid year ranges
        if 'year' in vertex_columns:
            year_col = vertices_table.column('year')
            min_year = pa.compute.min(year_col).as_py()
            max_year = pa.compute.max(year_col).as_py()
            
            if min_year < 1900 or max_year > 2030:
                issues.append(f"Suspicious year range: {min_year} to {max_year}")
        
        # Check edge connectivity
        if 'paper_id' in vertex_columns and 'source_id' in edge_columns and 'target_id' in edge_columns:
            vertex_ids = set(vertices_table.column('paper_id').to_pylist())
            source_ids = set(edges_table.column('source_id').to_pylist())
            target_ids = set(edges_table.column('target_id').to_pylist())
            
            edge_ids = source_ids.union(target_ids)
            orphaned_edges = edge_ids - vertex_ids
            
            if orphaned_edges:
                issues.append(f"Found {len(orphaned_edges)} edge IDs without corresponding vertices")
        
        return issues
    
    def get_data_statistics(self, vertices_table: pa.Table, edges_table: pa.Table) -> Dict[str, Any]:
        """
        Compute and return data statistics.
        
        Args:
            vertices_table: Vertices table
            edges_table: Edges table
            
        Returns:
            Dictionary of statistics
        """
        stats = {
            'vertices': {
                'count': vertices_table.num_rows,
                'columns': vertices_table.column_names,
                'memory_size_mb': vertices_table.nbytes / (1024 * 1024)
            },
            'edges': {
                'count': edges_table.num_rows,
                'columns': edges_table.column_names,
                'memory_size_mb': edges_table.nbytes / (1024 * 1024)
            }
        }
        
        # Year statistics
        if 'year' in vertices_table.column_names:
            year_col = vertices_table.column('year')
            stats['vertices']['year_range'] = {
                'min': pa.compute.min(year_col).as_py(),
                'max': pa.compute.max(year_col).as_py(),
                'mean': pa.compute.mean(year_col).as_py()
            }
        
        # Edge density
        vertex_count = vertices_table.num_rows
        edge_count = edges_table.num_rows
        if vertex_count > 1:
            max_edges = vertex_count * (vertex_count - 1)
            stats['graph'] = {
                'density': edge_count / max_edges,
                'avg_degree': (2 * edge_count) / vertex_count
            }
        
        return stats
    
    def cleanup_cache(self, keep_micro: bool = True) -> None:
        """
        Clean up cached data files.
        
        Args:
            keep_micro: Whether to keep micro benchmark data
        """
        cache_files = [
            "full_vertices.parquet",
            "full_edges.parquet"
        ]
        
        if not keep_micro:
            cache_files.extend([
                "micro_vertices.parquet",
                "micro_edges.parquet", 
                "micro_ids.parquet"
            ])
        
        for filename in cache_files:
            filepath = os.path.join(self.config.data.cache_dir, filename)
            if os.path.exists(filepath):
                os.remove(filepath)
                self.logger.info(f"Removed cache file: {filename}")
