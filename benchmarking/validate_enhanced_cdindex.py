#!/usr/bin/env python3
"""
Enhanced CD-Index Validation Script

Validates that the new enhanced CD-index implementation against
scores previously computed results

This script:
1. Loads the full graph from cached parquet data  
2. Reads previously computed scores from scores_all directory
3. Computes new scores using enhanced implementation
4. Compares scores with detailed reporting

Usage:
    python validate_enhanced_cdindex.py --part-id 0 --total-parts 100
"""

import os
import sys
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import argparse
import gzip
import datetime
import time
import psutil
import resource
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Add the parent directory to import the enhanced graph
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from fast_cdindex.cdindex_enhanced import EnhancedGraph

@dataclass
class ValidationStats:
    """Statistics for validation results."""
    total_papers: int = 0
    matched_papers: int = 0
    cdindex_matches: int = 0
    mcdindex_matches: int = 0
    iindex_matches: int = 0
    max_cdindex_diff: float = 0.0
    max_mcdindex_diff: float = 0.0
    max_iindex_diff: int = 0
    mismatched_papers: List[str] = None
    
    # Performance metrics
    computation_time_seconds: float = 0.0
    graph_load_time_seconds: float = 0.0
    papers_per_second: float = 0.0
    peak_memory_mb: float = 0.0
    avg_memory_mb: float = 0.0
    cpu_cores_used: int = 0
    total_cd_computations: int = 0
    cd_computations_per_second: float = 0.0
    
    def __post_init__(self):
        if self.mismatched_papers is None:
            self.mismatched_papers = []

class PerformanceMonitor:
    """Monitor performance metrics during validation."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = time.time()
        self.memory_samples = []
        self.computation_start = None
        self.cd_computation_count = 0
        
    def start_computation_timer(self):
        """Start timing the computation phase."""
        self.computation_start = time.time()
        
    def record_cd_computation(self):
        """Record a CD-index computation."""
        self.cd_computation_count += 1
        
    def sample_memory(self):
        """Sample current memory usage."""
        try:
            memory_info = self.process.memory_info()
            self.memory_samples.append(memory_info.rss / 1024 / 1024)  # Convert to MB
        except:
            pass
            
    def get_performance_stats(self) -> dict:
        """Get comprehensive performance statistics."""
        current_time = time.time()
        total_time = current_time - self.start_time
        computation_time = (current_time - self.computation_start) if self.computation_start else 0
        
        # Memory stats
        peak_memory = max(self.memory_samples) if self.memory_samples else 0
        avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
        
        # CPU info
        cpu_count = psutil.cpu_count()
        
        return {
            'computation_time_seconds': computation_time,
            'peak_memory_mb': peak_memory,
            'avg_memory_mb': avg_memory,
            'cpu_cores_used': cpu_count,
            'total_cd_computations': self.cd_computation_count,
            'cd_computations_per_second': self.cd_computation_count / computation_time if computation_time > 0 else 0,
            # PERFORMANCE FIX: Add papers_per_second calculation here instead of computed elsewhere
            'papers_per_second': 0.0  # Will be computed externally based on total_papers
        }

class ValidationResults:
    """Container for validation results and reporting."""
    
    def __init__(self, validate_cdindex_only: bool = False):
        self.stats = ValidationStats()
        self.detailed_mismatches = []
        self.tolerance = 1e-10
        self.logger = logging.getLogger(__name__)
        self.performance_monitor = PerformanceMonitor()
        self.validate_cdindex_only = validate_cdindex_only
    
    def compare_scores(self, paper_id: str, old_scores: dict, new_scores: dict) -> bool:
        """Compare old and new scores for a paper."""
        self.stats.total_papers += 1
        all_match = True
        mismatch_details = {'paper_id': paper_id}
        
        # Compare each metric with time windows
        all_comparisons = [
            ('cd3', 'cdindex', 3),
            ('cd5', 'cdindex', 5), 
            ('cd', 'cdindex', 150),
            ('mcd3', 'mcdindex', 3),
            ('mcd5', 'mcdindex', 5),
            ('mcd', 'mcdindex', 150),
            ('ncites3', 'iindex', 3),
            ('ncites5', 'iindex', 5),
            ('ncites', 'iindex', 150)
        ]
        
        # Filter comparisons based on validation mode
        if self.validate_cdindex_only:
            # Only validate CD-Index for faster validation
            comparisons = [
                ('cd3', 'cdindex', 3),
                ('cd5', 'cdindex', 5), 
                ('cd', 'cdindex', 150)
            ]
        else:
            comparisons = all_comparisons
        
        for old_key, new_method, years in comparisons:
            old_val = old_scores.get(old_key)
            new_val = new_scores.get(f"{new_method}_{years}")
            
            # Handle None values
            if old_val is None or pd.isna(old_val):
                if new_val is None or pd.isna(new_val):
                    continue  # Both None, that's fine
                else:
                    all_match = False
                    mismatch_details[f"{old_key}_mismatch"] = f"old=None, new={new_val}"
                    continue
            elif new_val is None or pd.isna(new_val):
                all_match = False
                mismatch_details[f"{old_key}_mismatch"] = f"old={old_val}, new=None"
                continue
            
            # Compare numerical values
            if 'cd' in old_key and old_key != 'ncites':  # CD and mCD indices (float)
                diff = abs(float(old_val) - float(new_val))
                if diff > self.tolerance:
                    all_match = False
                    mismatch_details[f"{old_key}_mismatch"] = f"old={old_val}, new={new_val}, diff={diff}"
                    
                    # Update max differences
                    if 'mcd' in old_key:
                        self.stats.max_mcdindex_diff = max(self.stats.max_mcdindex_diff, diff)
                    else:
                        self.stats.max_cdindex_diff = max(self.stats.max_cdindex_diff, diff)
                else:
                    # Count matches
                    if 'mcd' in old_key:
                        self.stats.mcdindex_matches += 1
                    else:
                        self.stats.cdindex_matches += 1
            else:  # iindex (integer)
                diff = abs(int(old_val) - int(new_val))
                if diff > 0:
                    all_match = False
                    mismatch_details[f"{old_key}_mismatch"] = f"old={old_val}, new={new_val}, diff={diff}"
                    self.stats.max_iindex_diff = max(self.stats.max_iindex_diff, diff)
                else:
                    self.stats.iindex_matches += 1
        
        if all_match:
            self.stats.matched_papers += 1
        else:
            self.stats.mismatched_papers.append(paper_id)
            self.detailed_mismatches.append(mismatch_details)
        
        return all_match
    
    def print_summary(self):
        """Print validation summary."""
        print("\n" + "="*80)
        print("ENHANCED CD-INDEX VALIDATION SUMMARY")
        print("="*80)
        print(f"Total papers validated: {self.stats.total_papers:,}")
        print(f"Perfectly matched papers: {self.stats.matched_papers:,}")
        print(f"Match rate: {100.0 * self.stats.matched_papers / max(1, self.stats.total_papers):.2f}%")
        print()
        print("Individual Metric Matches:")
        print(f"  CD-Index matches: {self.stats.cdindex_matches:,}")
        print(f"  mCD-Index matches: {self.stats.mcdindex_matches:,}")
        print(f"  I-Index matches: {self.stats.iindex_matches:,}")
        print()
        print("Maximum Differences:")
        print(f"  CD-Index max diff: {self.stats.max_cdindex_diff:.2e}")
        print(f"  mCD-Index max diff: {self.stats.max_mcdindex_diff:.2e}")
        print(f"  I-Index max diff: {self.stats.max_iindex_diff}")
        print()
        print("Performance Metrics:")
        print("="*80)
        print(f"Graph loading time: {self.stats.graph_load_time_seconds:.1f} seconds")
        print(f"Computation time: {self.stats.computation_time_seconds:.1f} seconds")
        print(f"Papers per second: {self.stats.papers_per_second:.1f}")
        print(f"Total CD computations: {self.stats.total_cd_computations:,}")
        print(f"CD computations per second: {self.stats.cd_computations_per_second:.1f}")
        print(f"Peak memory usage: {self.stats.peak_memory_mb:.1f} MB")
        print(f"Average memory usage: {self.stats.avg_memory_mb:.1f} MB")
        print(f"CPU cores available: {self.stats.cpu_cores_used}")
        print()
        
        if len(self.stats.mismatched_papers) > 0:
            print(f"Mismatched papers: {len(self.stats.mismatched_papers)}")
            print("First 10 mismatched papers:")
            for paper_id in self.stats.mismatched_papers[:10]:
                print(f"  {paper_id}")
            
            if len(self.detailed_mismatches) > 0:
                print("\nFirst 5 detailed mismatches:")
                for i, mismatch in enumerate(self.detailed_mismatches[:5]):
                    print(f"  {i+1}. Paper {mismatch['paper_id']}:")
                    for key, value in mismatch.items():
                        if key != 'paper_id':
                            print(f"     {key}: {value}")
        
        print("="*80)

class EnhancedValidation:
    """Main validation class."""
    
    def __init__(self, data_cache_dir: str, scores_dir: str, part_id: int = 0, total_parts: int = 1,
                 use_tsv: bool = False, wos_root: Optional[str] = None, limit_papers: Optional[int] = None,
                 tsv_vertex_dir: Optional[str] = None, tsv_edge_dir: Optional[str] = None,
                 legacy_mode: bool = False, write_cache_dir: Optional[str] = None,
                 reuse_cache_dir: Optional[str] = None, validate_cdindex_only: bool = False,
                 max_files: Optional[int] = None):
        self.data_cache_dir = data_cache_dir
        self.scores_dir = scores_dir
        self.part_id = part_id
        self.total_parts = total_parts
        self.logger = logging.getLogger(__name__)
        self.results = ValidationResults(validate_cdindex_only)
        self.graph = None
        self.id_mapping = None  # WoS ID to integer ID mapping (used only for cache path)
        self.uid2id: Dict[str, int] = {}  # used for TSV path
        self.use_tsv = use_tsv
        self.wos_root = wos_root or "/project/jevans/tip/disruption/code_wos_2023/WoS_data"
        self.tsv_vertex_dir = tsv_vertex_dir or os.path.join(self.wos_root, "paper_years.tsv")
        self.tsv_edge_dir = tsv_edge_dir or os.path.join(self.wos_root, "edges.tsv")
        self.limit_papers = limit_papers
        self.legacy_mode = legacy_mode
        self.validate_cdindex_only = validate_cdindex_only
        self.max_files = max_files
        
        # Time windows (in years, enhanced implementation uses year arithmetic)
        self.time_windows = {3: 3, 5: 5, 150: 150}
        # Legacy windows in seconds (with leap-day adjustments used previously)
        self.seconds_windows = {
            3: int(datetime.timedelta(days=365*3 + 1).total_seconds()),
            5: int(datetime.timedelta(days=365*5 + 2).total_seconds()),
            150: int(datetime.timedelta(days=365*150).total_seconds()),
        }

        # Optional parquet cache output/input for TSV-built graphs
        self.write_cache_dir = write_cache_dir
        self.reuse_cache_dir = reuse_cache_dir

    def load_id_mapping(self):
        """Load WoS ID to integer ID mapping."""
        mapping_cache = os.path.join(self.data_cache_dir, "id_mapping.parquet")
        
        if os.path.exists(mapping_cache):
            self.logger.info("Loading ID mapping...")
            mapping_table = pq.read_table(mapping_cache)
            mapping_df = mapping_table.to_pandas()
            
            # Convert to dictionary for fast lookup (WoS ID -> integer ID)
            self.id_mapping = dict(zip(mapping_df['paper_id'], mapping_df['id']))
            self.logger.info(f"Loaded ID mapping for {len(self.id_mapping):,} papers")
        else:
            self.logger.warning(f"ID mapping not found at {mapping_cache}")
            self.id_mapping = {}
    
    def load_graph_from_cache(self) -> EnhancedGraph:
        """Load the full graph from cached parquet files."""
        self.logger.info("Loading graph from cached parquet data...")
        
        # Load ID mapping first
        self.load_id_mapping()
        
        # Check for cached files (can be directories with partitioned parquet files)
        vertices_cache = os.path.join(self.data_cache_dir, "paper_years.parquet")
        edges_cache = os.path.join(self.data_cache_dir, "edges.parquet")
        
        if not os.path.exists(vertices_cache):
            raise FileNotFoundError(f"Vertices cache not found: {vertices_cache}")
        if not os.path.exists(edges_cache):
            raise FileNotFoundError(f"Edges cache not found: {edges_cache}")
        
        # Load parquet files (handle both single files and partitioned directories)
        self.logger.info("Loading vertices...")
        if os.path.isdir(vertices_cache):
            # Partitioned dataset
            self.logger.info(f"Loading partitioned vertices dataset from {vertices_cache}")
            vertices_dataset = pq.ParquetDataset(vertices_cache)
            vertices_table = vertices_dataset.read()
        else:
            # Single file
            vertices_table = pq.read_table(vertices_cache)
        self.logger.info(f"Loaded {vertices_table.num_rows:,} vertices")
        
        self.logger.info("Loading edges...")  
        if os.path.isdir(edges_cache):
            # Partitioned dataset
            self.logger.info(f"Loading partitioned edges dataset from {edges_cache}")
            edges_dataset = pq.ParquetDataset(edges_cache)
            edges_table = edges_dataset.read()
        else:
            # Single file
            edges_table = pq.read_table(edges_cache)
        self.logger.info(f"Loaded {edges_table.num_rows:,} edges")
        
        # Create and populate EnhancedGraph
        graph = EnhancedGraph()
        
        self.logger.info("Adding vertices to graph...")
        graph.add_vertices_from_arrow(vertices_table)
        
        self.logger.info("Adding vertices to PropertyStore for year bitmaps...")
        graph.properties.ingest_arrow(vertices_table)
        
        self.logger.info("Building PropertyStore indexes for window bitmap optimization...")
        graph.properties.build_indexes()
        
        self.logger.info("Adding edges to graph...")
        graph.add_edges_from_arrow(edges_table)
        
        self.logger.info("Graph construction complete")
        self.logger.info(f"Final graph: {graph.vertex_count():,} vertices, {graph.edge_count():,} edges")
        
        return graph

    def _build_uid_mapping_from_tsv(self):
        """Build a consistent UID->int mapping by scanning the vertex TSV directory."""
        self.logger.info(f"Building UID mapping from {self.tsv_vertex_dir} ...")
        uid_set = []
        vertex_files = [f for f in os.listdir(self.tsv_vertex_dir) if f.endswith('.csv')]
        vertex_files.sort()
        for vf in vertex_files:
            vpath = os.path.join(self.tsv_vertex_dir, vf)
            with open(vpath, 'rt') as fh:
                for line in fh:
                    parts = line.rstrip('\n').split('\t')
                    if len(parts) < 2:
                        continue
                    uid_str = parts[0]
                    uid_set.append(uid_str)
        # Assign deterministic IDs in file order
        self.uid2id = {uid: idx for idx, uid in enumerate(uid_set)}
        self.logger.info(f"UID mapping size: {len(self.uid2id):,}")

    def load_graph_from_tsv(self) -> EnhancedGraph:
        """Build graph from TSV inputs like compute_cd_one.py (UID->year; source->target edges)."""
        self.logger.info("Building graph from TSV directories (compute_cd_one compatible)...")
        if not os.path.isdir(self.tsv_vertex_dir):
            raise FileNotFoundError(f"Vertices TSV directory not found: {self.tsv_vertex_dir}")
        if not os.path.isdir(self.tsv_edge_dir):
            raise FileNotFoundError(f"Edges TSV directory not found: {self.tsv_edge_dir}")

        g = EnhancedGraph()
        self.uid2id = {}
        next_id = 0
        
        # Ingest vertices and assign mapping deterministically in file order
        self.logger.info(f"Ingesting vertices from {self.tsv_vertex_dir}...")
        vertex_files = [f for f in os.listdir(self.tsv_vertex_dir) if (f.endswith('.csv') or f.endswith('.csv.gz'))]
        vertex_files.sort()
        added_vertices = 0
        uids: List[int] = []
        years: List[int] = []
        for vf in vertex_files:
            vpath = os.path.join(self.tsv_vertex_dir, vf)
            opener = gzip.open if vf.endswith('.gz') else open
            mode = 'rt'
            with opener(vpath, mode) as fh:
                for line in fh:
                    parts = line.rstrip('\n').split('\t')
                    if len(parts) < 2:
                        continue
                    uid_str, year_str = parts[0], parts[1]
                    if uid_str not in self.uid2id:
                        self.uid2id[uid_str] = next_id
                        next_id += 1
                    pid = self.uid2id[uid_str]
                    try:
                        yr = int(year_str)
                    except:
                        continue
                    uids.append(pid)
                    years.append(yr)
                    added_vertices += 1
                    if len(uids) >= 1_000_000:
                        table = pa.table({"paper_id": pa.array(uids, type=pa.uint32()),
                                          "year": pa.array(years, type=pa.int32())})
                        g.add_vertices_from_arrow(table)
                        # Also add to PropertyStore for year bitmaps
                        g.properties.ingest_arrow(table)
                        uids.clear(); years.clear()
        if uids:
            table = pa.table({"paper_id": pa.array(uids, type=pa.uint32()),
                              "year": pa.array(years, type=pa.int32())})
            g.add_vertices_from_arrow(table)
            # Also add to PropertyStore for year bitmaps
            g.properties.ingest_arrow(table)
            uids.clear(); years.clear()
        self.logger.info(f"Assigned {len(self.uid2id):,} UIDs, added vertices: {added_vertices:,}")
        
        # Build PropertyStore indexes for window bitmap optimization
        self.logger.info("Building PropertyStore indexes for year bitmaps...")
        g.properties.build_indexes()

        # Ingest edges
        self.logger.info(f"Ingesting edges from {self.tsv_edge_dir}...")
        edge_files = [f for f in os.listdir(self.tsv_edge_dir) if (f.endswith('.csv') or f.endswith('.csv.gz'))]
        edge_files.sort()
        batch_source: List[int] = []
        batch_target: List[int] = []
        added_edges = 0
        for ef in edge_files:
            epath = os.path.join(self.tsv_edge_dir, ef)
            opener = gzip.open if ef.endswith('.gz') else open
            mode = 'rt'
            with opener(epath, mode) as fh:
                for line in fh:
                    parts = line.rstrip('\n').split('\t')
                    if len(parts) < 2:
                        continue
                    target_uid, source_uid = parts[0], parts[1]  # matches compute_cd_one.py
                    if source_uid not in self.uid2id or target_uid not in self.uid2id:
                        continue
                    s = self.uid2id[source_uid]
                    t = self.uid2id[target_uid]
                    batch_source.append(s)
                    batch_target.append(t)
                    added_edges += 1
                    if len(batch_source) >= 1_000_000:
                        et = pa.table({"source_id": pa.array(batch_source, type=pa.uint32()),
                                       "target_id": pa.array(batch_target, type=pa.uint32())})
                        g.add_edges_from_arrow(et)
                        batch_source.clear(); batch_target.clear()
        if batch_source:
            et = pa.table({"source_id": pa.array(batch_source, type=pa.uint32()),
                           "target_id": pa.array(batch_target, type=pa.uint32())})
            g.add_edges_from_arrow(et)
            batch_source.clear(); batch_target.clear()
        self.logger.info(f"Added edges: {added_edges:,}")

        # Optionally write parquet cache (second pass, streaming TSV again)
        if self.write_cache_dir:
            self.logger.info(f"Writing TSV parquet cache to {self.write_cache_dir}")
            os.makedirs(self.write_cache_dir, exist_ok=True)
            # Write vertices
            years_map: Dict[int, int] = {}
            vertex_files = [f for f in os.listdir(self.tsv_vertex_dir) if (f.endswith('.csv') or f.endswith('.csv.gz'))]
            vertex_files.sort()
            for vf in vertex_files:
                vpath = os.path.join(self.tsv_vertex_dir, vf)
                opener = gzip.open if vf.endswith('.gz') else open
                with opener(vpath, 'rt') as fh:
                    for line in fh:
                        parts = line.rstrip('\n').split('\t')
                        if len(parts) < 2:
                            continue
                        uid_str, year_str = parts[0], parts[1]
                        if uid_str in self.uid2id:
                            try:
                                years_map[self.uid2id[uid_str]] = int(year_str)
                            except:
                                pass
            ids_sorted = sorted(self.uid2id.values())
            years_array = [years_map.get(i, 0) for i in ids_sorted]
            vert_tbl = pa.table({
                'paper_id': pa.array(ids_sorted, type=pa.uint32()),
                'year': pa.array(years_array, type=pa.int32()),
            })
            pq.write_table(vert_tbl, os.path.join(self.write_cache_dir, 'paper_years.parquet'))

            # Write edges
            edge_src: List[int] = []
            edge_tgt: List[int] = []
            edge_files = [f for f in os.listdir(self.tsv_edge_dir) if (f.endswith('.csv') or f.endswith('.csv.gz'))]
            edge_files.sort()
            for ef in edge_files:
                epath = os.path.join(self.tsv_edge_dir, ef)
                opener = gzip.open if ef.endswith('.gz') else open
                with opener(epath, 'rt') as fh:
                    for line in fh:
                        parts = line.rstrip('\n').split('\t')
                        if len(parts) < 2:
                            continue
                        target_uid, source_uid = parts[0], parts[1]
                        if source_uid in self.uid2id and target_uid in self.uid2id:
                            edge_src.append(self.uid2id[source_uid])
                            edge_tgt.append(self.uid2id[target_uid])
            edge_tbl = pa.table({
                'source_id': pa.array(edge_src, type=pa.uint32()),
                'target_id': pa.array(edge_tgt, type=pa.uint32()),
            })
            pq.write_table(edge_tbl, os.path.join(self.write_cache_dir, 'edges.parquet'))

            # Write id mapping (UID -> int id)
            id_map_tbl = pa.table({
                'wos_id': pa.array(list(self.uid2id.keys()), type=pa.string()),
                'id': pa.array(list(self.uid2id.values()), type=pa.int32()),
            })
            pq.write_table(id_map_tbl, os.path.join(self.write_cache_dir, 'id_mapping.parquet'))

        return g

    def load_graph_from_cache_dir(self, cache_dir: str) -> EnhancedGraph:
        self.logger.info(f"Loading graph from TSV parquet cache: {cache_dir}")
        g = EnhancedGraph()
        vpath = os.path.join(cache_dir, 'paper_years.parquet')
        epath = os.path.join(cache_dir, 'edges.parquet')
        if not (os.path.exists(vpath) and os.path.exists(epath)):
            raise FileNotFoundError("Cache dir missing paper_years.parquet or edges.parquet")
        vert_tbl = pq.read_table(vpath)
        edge_tbl = pq.read_table(epath)
        g.add_vertices_from_arrow(vert_tbl)
        
        # Also add vertices to PropertyStore for year bitmaps
        self.logger.info("Adding vertices to PropertyStore for year bitmaps...")
        g.properties.ingest_arrow(vert_tbl)
        g.properties.build_indexes()
        
        g.add_edges_from_arrow(edge_tbl)
        # Load id map if present
        mpath = os.path.join(cache_dir, 'id_mapping.parquet')
        if os.path.exists(mpath):
            mt = pq.read_table(mpath)
            keys = mt.column('wos_id').to_pylist()
            vals = mt.column('id').to_pylist()
            self.uid2id = dict(zip(keys, vals))
        return g

    def _year_to_epoch_seconds(self, year: int) -> int:
        return int(datetime.datetime(year, 1, 1, tzinfo=datetime.timezone.utc).timestamp())

    def _legacy_cdindex(self, pid: int, years: int) -> Dict[str, float]:
        """Legacy cdindex (cdindex.cpp) using seconds windows and union-of-citers."""
        ft_year = self.graph.get_timestamp(pid)
        ft_sec = self._year_to_epoch_seconds(ft_year)
        dt_sec = self.seconds_windows[years]
        tmax = ft_sec + dt_sec

        preds = set(self.graph.out_edges(pid))

        # Citers of focal within (ft, tmax]
        citers_f = set()
        for c in self.graph.in_edges(pid):
            cy = self._year_to_epoch_seconds(self.graph.get_timestamp(c))
            if cy > ft_sec and cy <= tmax:
                citers_f.add(c)

        # Citers of predecessors within (ft, tmax]
        citers_b = set()
        for b in preds:
            for c in self.graph.in_edges(b):
                cy = self._year_to_epoch_seconds(self.graph.get_timestamp(c))
                if cy > ft_sec and cy <= tmax:
                    citers_b.add(c)

        it_set = citers_f | citers_b
        n_t = len(it_set)
        if n_t == 0:
            return {"cd": 0.0, "mcd": 0.0, "iindex": 0}

        # PERFORMANCE FIX: Cache outgoing edges to avoid quadratic set operations in legacy mode
        citer_out_edges = {}  # Cache outgoing edges for each citer
        
        sum_terms = 0.0
        for i in it_set:
            # Cache outgoing edges for this citer to avoid repeated set() calls
            if i not in citer_out_edges:
                citer_out_edges[i] = set(self.graph.out_edges(i))
            
            out_edges_i = citer_out_edges[i]
            
            # f_i: does i cite focal?
            f_i = 1 if pid in out_edges_i else 0
            # b_i: does i cite any predecessor?
            b_i = 1 if len(out_edges_i & preds) > 0 else 0
            sum_terms += (-2.0 * f_i * b_i + f_i)

        cd = sum_terms / n_t

        # Legacy iindex: citers of focal with citer_time <= tmax
        iidx = 0
        for c in self.graph.in_edges(pid):
            cy = self._year_to_epoch_seconds(self.graph.get_timestamp(c))
            if cy <= tmax:
                iidx += 1

        mcd = cd * iidx
        return {"cd": cd, "mcd": mcd, "iindex": iidx}

    def get_score_files(self) -> List[str]:
        """Get list of score files to process."""
        import math
        
        all_files = [f for f in os.listdir(self.scores_dir) if f.endswith('.csv.gz')]
        all_files.sort()
        
        # TESTING: Process from beginning for production-grade validation test
        total_available = len(all_files)
        skip_first_n = 0  # No skipping for testing
        
        if len(all_files) > skip_first_n and skip_first_n > 0:
            all_files = all_files[skip_first_n:]
            self.logger.info(f"Skipped first {skip_first_n} files (already validated)")
        else:
            self.logger.info(f"Processing from beginning - no files skipped")
        
        # Limit to first N files if max_files is specified  
        if self.max_files and self.max_files > 0:
            all_files = all_files[:self.max_files]
            if skip_first_n > 0:
                self.logger.info(f"Limited to next {self.max_files} files (files {skip_first_n+1}-{skip_first_n+len(all_files)}) out of {total_available} total")
            else:
                self.logger.info(f"Limited to first {self.max_files} files (files 1-{len(all_files)}) out of {total_available} total")
        
        # Partition files across multiple jobs with proper load balancing
        files_per_part = math.ceil(len(all_files) / self.total_parts)
        start_idx = self.part_id * files_per_part
        end_idx = min(start_idx + files_per_part, len(all_files))
        
        selected_files = all_files[start_idx:end_idx]
        
        # Optionally shorten for small-sample validation
        if self.limit_papers:
            selected_files = selected_files[:1]  # process only first file for sampling
        
        self.logger.info(f"Processing part {self.part_id+1}/{self.total_parts}")
        self.logger.info(f"Total files after skip+limit: {len(all_files)}, this part: {len(selected_files)}")
        if selected_files:
            # Show actual file numbers in the original sequence
            first_file_num = skip_first_n + 1 + start_idx
            last_file_num = skip_first_n + start_idx + len(selected_files)
            self.logger.info(f"Processing files #{first_file_num}-{last_file_num}: {selected_files[0]} to {selected_files[-1]}")
        else:
            self.logger.info("No files assigned to this part")
        
        return selected_files
    
    def compute_new_scores(self, paper_id: str) -> Dict[str, float]:
        """Compute new scores using enhanced implementation or legacy diagnostic."""
        scores = {}
        try:
            # Map WoS UID to internal integer
            if self.use_tsv:
                if isinstance(paper_id, str) and paper_id in self.uid2id:
                    pid = self.uid2id[paper_id]
                else:
                    for years in self.time_windows.keys():
                        scores[f"cdindex_{years}"] = None
                        scores[f"mcdindex_{years}"] = None
                        scores[f"iindex_{years}"] = None
                    return scores
            else:
                if isinstance(paper_id, str) and paper_id.startswith('WOS:'):
                    if self.id_mapping and paper_id in self.id_mapping:
                        pid = self.id_mapping[paper_id]
                    else:
                        for years in self.time_windows.keys():
                            scores[f"cdindex_{years}"] = None
                            scores[f"mcdindex_{years}"] = None
                            scores[f"iindex_{years}"] = None
                        return scores
                else:
                    pid = int(paper_id)

            if self.legacy_mode:
                for years, dt in self.time_windows.items():
                    legacy = self._legacy_cdindex(pid, years)
                    scores[f"cdindex_{years}"] = legacy["cd"]
                    scores[f"mcdindex_{years}"] = legacy["mcd"]
                    scores[f"iindex_{years}"] = legacy["iindex"]
                    # PERFORMANCE FIX: Count only actual computations (legacy does cd+iindex in one pass)
                    self.results.performance_monitor.record_cd_computation()  # One computation per window
            else:
                if self.validate_cdindex_only:
                    for years in self.time_windows:
                        scores[f"cdindex_{years}"] = self.graph.cdindex(pid, years)
                        # PERFORMANCE FIX: Count each cdindex call once (not multiple times)
                        self.results.performance_monitor.record_cd_computation()
                    return scores
                else:
                    for years in self.time_windows:
                        cd  = self.graph.cdindex(pid, years)
                        iix = self.graph.iindex(pid, years)
                        scores[f"cdindex_{years}"] = cd
                        scores[f"iindex_{years}"]  = iix
                        scores[f"mcdindex_{years}"] = cd * iix # ← don't call C++ mcdindex (it re-calls cdindex)
                        # PERFORMANCE FIX: Count actual C++ calls (cdindex + iindex), not the Python multiplication
                        self.results.performance_monitor.record_cd_computation()  # cdindex call
                        self.results.performance_monitor.record_cd_computation()  # iindex call
        except Exception as e:
            self.logger.debug(f"Failed to compute scores for {paper_id}: {e}")
            for years in self.time_windows.keys():
                scores[f"cdindex_{years}"] = None
                scores[f"mcdindex_{years}"] = None
                scores[f"iindex_{years}"] = None
        return scores
    
    def validate_score_file(self, filename: str):
        """Validate a single score file."""
        filepath = os.path.join(self.scores_dir, filename)
        self.logger.info(f"Processing {filename}")
        
        # Create output CSV file for computed scores 
        results_dir = os.path.join(os.path.dirname(__file__), 'validation_results')
        os.makedirs(results_dir, exist_ok=True)
        
        # Generate output filename matching scores_all format 
        output_filename = f"enhanced_{filename}"
        output_filepath = os.path.join(results_dir, output_filename)
        
        try:
            # PERFORMANCE FIX: Use pandas built-in compression and pyarrow engine for faster I/O
            # Only read columns we need for cdindex-only mode
            if self.validate_cdindex_only:
                usecols = ['UID', 'nrefs', 'cd3', 'cd5', 'cd']
            else:
                usecols = ['UID', 'nrefs', 'cd3', 'mcd3', 'ncites3', 'cd5', 'mcd5', 'ncites5', 'cd', 'mcd', 'ncites']
            
            try:
                df = pd.read_csv(filepath, sep='\t', compression='gzip', 
                               engine='pyarrow', usecols=usecols, dtype={'UID': 'string'})
            except ImportError:
                # Fallback if pyarrow not available
                df = pd.read_csv(filepath, sep='\t', compression='gzip', usecols=usecols)
            
            self.logger.info(f"Loaded {len(df)} records from {filename}")
            
            # If limiting papers, truncate DataFrame
            if self.limit_papers:
                df = df.head(self.limit_papers)
            
            # Prepare CSV output file for computed scores
            computed_scores_data = []
            
            # PERFORMANCE FIX: Use itertuples instead of iterrows + row.to_dict()
            # This eliminates Python object creation on every row
            for idx, row in enumerate(df.itertuples(index=False, name="Row")):
                if idx % 1000 == 0:
                    self.logger.info(f"Processed {idx:,} papers in {filename}")
                    # Sample memory usage periodically
                    self.results.performance_monitor.sample_memory()
                
                paper_id = row.UID
                # PERFORMANCE FIX: Create lightweight dict instead of full row.to_dict()
                old_scores = row._asdict()
                new_scores = self.compute_new_scores(paper_id)
                
                # Compare scores for validation
                self.results.compare_scores(paper_id, old_scores, new_scores)
                
                # Collect computed scores for CSV output
                # Format matches scores_all structure: UID, nrefs, cd3, mcd3, ncites3, cd5, mcd5, ncites5, cd, mcd, ncites
                computed_row = {
                    'UID': paper_id,
                    'nrefs': row.nrefs,  # Keep original nrefs
                    'cd3': new_scores.get('cdindex_3', 0.0),
                    'mcd3': (0.0 if self.validate_cdindex_only else new_scores.get('mcdindex_3', 0.0)),
                    'ncites3': (0 if self.validate_cdindex_only else new_scores.get('iindex_3', 0)),
                    'cd5': new_scores.get('cdindex_5', 0.0),
                    'mcd5': (0.0 if self.validate_cdindex_only else new_scores.get('mcdindex_5', 0.0)),
                    'ncites5': (0 if self.validate_cdindex_only else new_scores.get('iindex_5', 0)),
                    'cd': new_scores.get('cdindex_150', 0.0),
                    'mcd': (0.0 if self.validate_cdindex_only else new_scores.get('mcdindex_150', 0.0)),
                    'ncites': (0 if self.validate_cdindex_only else new_scores.get('iindex_150', 0))
                }
                computed_scores_data.append(computed_row)
            
            # Write computed scores to CSV file - PERFORMANCE FIX: Use pandas compression
            if computed_scores_data:
                computed_df = pd.DataFrame(computed_scores_data)
                computed_df.to_csv(output_filepath, sep='\t', index=False, 
                                 float_format='%.15g', compression='gzip')
                self.logger.info(f"Wrote {len(computed_scores_data)} computed scores to {output_filename}")
                
        except Exception as e:
            self.logger.error(f"Error processing {filename}: {e}")
    
    def run_validation(self):
        """Run the complete validation."""
        start_time = time.time()
        
        self.logger.info("Starting Enhanced CD-Index Validation")
        self.logger.info(f"Part {self.part_id+1} of {self.total_parts}")
        
        # Load graph
        if self.reuse_cache_dir:
            self.graph = self.load_graph_from_cache_dir(self.reuse_cache_dir)
        else:
            if self.use_tsv:
                self.graph = self.load_graph_from_tsv()
            else:
                self.graph = self.load_graph_from_cache()
        graph_load_time = time.time() - start_time
        self.logger.info(f"Graph loaded in {graph_load_time:.1f} seconds")
        
        # PERFORMANCE FIX: Call prepare_for_searching() once after graph loading
        # This sorts all incoming edges by time - doing it once instead of on every CD computation
        self.logger.info("Preparing graph for efficient searching (sorting edges)...")
        prep_start = time.time()
        self.graph.prepare_for_searching()
        prep_time = time.time() - prep_start
        self.logger.info(f"Graph preparation completed in {prep_time:.1f} seconds")
        
        # Reset micro-benchmarking counters
        self.logger.info("Resetting micro-benchmark counters...")
        self.graph.reset_benchmark()
        
        # Start computation phase timing
        self.results.performance_monitor.start_computation_timer()
        
        # Get files to process
        score_files = self.get_score_files()
        
        if not score_files:
            self.logger.warning("No score files to process")
            return
        
        # Process each file
        for i, filename in enumerate(score_files):
            file_start = time.time()
            self.validate_score_file(filename)
            file_time = time.time() - file_start
            self.logger.info(f"Completed {filename} in {file_time:.1f} seconds")
            
            # SAVE INTERMEDIATE RESULTS after each file (critical for long-running jobs)
            try:
                self.save_intermediate_results(i + 1, len(score_files))
                self.logger.info(f"Intermediate results saved after processing {i + 1}/{len(score_files)} files")
            except Exception as e:
                self.logger.warning(f"Failed to save intermediate results: {e}")
        
        total_time = time.time() - start_time
        self.logger.info(f"Validation completed in {total_time:.1f} seconds")
        
        # Update performance stats
        perf_stats = self.results.performance_monitor.get_performance_stats()
        self.results.stats.graph_load_time_seconds = graph_load_time
        self.results.stats.computation_time_seconds = perf_stats['computation_time_seconds']
        self.results.stats.papers_per_second = self.results.stats.total_papers / perf_stats['computation_time_seconds'] if perf_stats['computation_time_seconds'] > 0 else 0
        self.results.stats.peak_memory_mb = perf_stats['peak_memory_mb']
        self.results.stats.avg_memory_mb = perf_stats['avg_memory_mb']
        self.results.stats.cpu_cores_used = perf_stats['cpu_cores_used']
        self.results.stats.total_cd_computations = perf_stats['total_cd_computations']
        self.results.stats.cd_computations_per_second = perf_stats['cd_computations_per_second']
        
        # Print results
        self.results.print_summary()
        
        # Print micro-benchmark results
        self.logger.info("Printing micro-benchmark results...")
        self.graph.print_benchmark_summary()
        
        # Save detailed results
        self.save_results()
    
    def save_intermediate_results(self, files_completed, total_files):
        """Save intermediate validation results after each file completion."""
        results_dir = os.path.join(os.path.dirname(__file__), 'validation_results')
        os.makedirs(results_dir, exist_ok=True)
        
        # Save intermediate summary
        intermediate_file = os.path.join(results_dir, f'validation_intermediate_part_{self.part_id:03d}.txt')
        perf_stats = self.results.performance_monitor.get_performance_stats()
        
        with open(intermediate_file, 'w') as f:
            f.write(f"Enhanced CD-Index Validation - INTERMEDIATE RESULTS\n")
            f.write(f"Part {self.part_id+1}/{self.total_parts} - Progress: {files_completed}/{total_files} files\n")
            f.write(f"Generated: {datetime.datetime.now()}\n\n")
            f.write(f"Current status: {100.0 * files_completed / total_files:.1f}% complete\n\n")
            
            if self.results.stats.total_papers > 0:
                f.write(f"Papers processed so far: {self.results.stats.total_papers:,}\n")
                f.write(f"Papers matched: {self.results.stats.matched_papers:,}\n")
                f.write(f"Current match rate: {100.0 * self.results.stats.matched_papers / self.results.stats.total_papers:.2f}%\n\n")
                
                # Performance so far - PERFORMANCE FIX: Compute papers_per_second correctly
                computation_time = perf_stats.get('computation_time_seconds', 1)
                papers_per_sec = self.results.stats.total_papers / computation_time if computation_time > 0 else 0
                
                f.write("Performance (current):\n")
                f.write(f"Papers per second: {papers_per_sec:.1f}\n")
                f.write(f"CD computations per second: {perf_stats.get('cd_computations_per_second', 0):.1f}\n")
                f.write(f"Peak memory: {perf_stats.get('peak_memory_mb', 0):.1f} MB\n")
                f.write(f"CPU cores used: {perf_stats.get('cpu_cores_used', 0)}\n\n")
            
            f.write(f"Last updated after file #{files_completed}\n")
    
    def save_results(self):
        """Save validation results to file."""
        results_dir = os.path.join(os.path.dirname(__file__), 'validation_results')
        os.makedirs(results_dir, exist_ok=True)
        
        # Save summary
        summary_file = os.path.join(results_dir, f'validation_summary_part_{self.part_id:03d}.txt')
        with open(summary_file, 'w') as f:
            f.write(f"Enhanced CD-Index Validation Summary - Part {self.part_id+1}/{self.total_parts}\n")
            f.write(f"Generated: {datetime.datetime.now()}\n\n")
            f.write(f"Total papers validated: {self.results.stats.total_papers:,}\n")
            f.write(f"Perfectly matched papers: {self.results.stats.matched_papers:,}\n")
            f.write(f"Match rate: {100.0 * self.results.stats.matched_papers / max(1, self.results.stats.total_papers):.2f}%\n\n")
            f.write(f"CD-Index matches: {self.results.stats.cdindex_matches:,}\n")
            f.write(f"mCD-Index matches: {self.results.stats.mcdindex_matches:,}\n")
            f.write(f"I-Index matches: {self.results.stats.iindex_matches:,}\n\n")
            f.write(f"Max CD-Index diff: {self.results.stats.max_cdindex_diff:.2e}\n")
            f.write(f"Max mCD-Index diff: {self.results.stats.max_mcdindex_diff:.2e}\n")
            f.write(f"Max I-Index diff: {self.results.stats.max_iindex_diff}\n\n")
            
            # Performance metrics
            f.write("Performance Metrics:\n")
            f.write("="*50 + "\n")
            f.write(f"Graph loading time: {self.results.stats.graph_load_time_seconds:.1f} seconds\n")
            f.write(f"Computation time: {self.results.stats.computation_time_seconds:.1f} seconds\n")
            f.write(f"Papers per second: {self.results.stats.papers_per_second:.1f}\n")
            f.write(f"Total CD computations: {self.results.stats.total_cd_computations:,}\n")
            f.write(f"CD computations per second: {self.results.stats.cd_computations_per_second:.1f}\n")
            f.write(f"Peak memory usage: {self.results.stats.peak_memory_mb:.1f} MB\n")
            f.write(f"Average memory usage: {self.results.stats.avg_memory_mb:.1f} MB\n")
            f.write(f"CPU cores available: {self.results.stats.cpu_cores_used}\n\n")
            
            if self.results.stats.mismatched_papers:
                f.write("Mismatched papers:\n")
                for paper_id in self.results.stats.mismatched_papers:
                    f.write(f"  {paper_id}\n")
            
            # Note about generated CSV files
            f.write("\nOutput Files:\n")
            f.write("="*50 + "\n")
            f.write("Computed CD-index scores have been written to compressed CSV files\n")
            f.write("matching the format of scores_all/ directory:\n")
            f.write(f"  Files prefixed with 'enhanced_' in {results_dir}/\n")
            f.write("  Format: UID, nrefs, cd3, mcd3, ncites3, cd5, mcd5, ncites5, cd, mcd, ncites\n")
            f.write("  These files can be used for direct comparison with legacy scores.\n")
        
        # Save detailed mismatches if any
        if self.results.detailed_mismatches:
            mismatches_file = os.path.join(results_dir, f'detailed_mismatches_part_{self.part_id:03d}.csv')
            pd.DataFrame(self.results.detailed_mismatches).to_csv(mismatches_file, index=False)
            
        self.logger.info(f"Results saved to {results_dir}")
        self.logger.info("Computed scores written to enhanced_*.csv.gz files for comparison")

def main():
    parser = argparse.ArgumentParser(description='Validate Enhanced CD-Index Implementation')
    parser.add_argument('--data-cache', required=True, 
                       help='Path to data cache directory with parquet files')
    parser.add_argument('--scores-dir', required=True,
                       help='Path to directory with original score files')
    parser.add_argument('--part-id', type=int, default=0,
                       help='Part ID for parallel processing (0-based)')
    parser.add_argument('--total-parts', type=int, default=1,
                       help='Total number of parts for parallel processing')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    # New options
    parser.add_argument('--use-tsv', action='store_true', help='Build graph from WoS TSV data like compute_cd_one.py')
    parser.add_argument('--wos-root', default='/project/jevans/tip/disruption/code_wos_2023/WoS_data', help='WoS data root')
    parser.add_argument('--limit-papers', type=int, default=None, help='Validate only first N papers from selected score file')
    parser.add_argument('--tsv-vertex-dir', default=None, help='Override TSV vertex dir (defaults to <wos_root>/paper_years.tsv)')
    parser.add_argument('--tsv-edge-dir', default=None, help='Override TSV edge dir (defaults to <wos_root>/edges.tsv)')
    parser.add_argument('--legacy-mode', action='store_true', help='Run in legacy mode (seconds windows, union-of-citers)')
    parser.add_argument('--write-cache-dir', default=None, help='If set, write a parquet cache after TSV build')
    parser.add_argument('--reuse-cache-dir', default=None, help='If set, load graph from this parquet cache dir instead of TSV')
    parser.add_argument('--quick-benchmark', action='store_true', help='Run a quick micro-benchmark with limited papers')
    parser.add_argument('--validate-cdindex-only', action='store_true', help='Validate only CD-Index, skip mCD and IIndex for faster validation')
    parser.add_argument('--max-files', type=int, default=None, help='Limit to first N score files (out of 200 total)')

    args = parser.parse_args()
    
    # Quick benchmark mode: automatically set a small paper limit
    if args.quick_benchmark and not args.limit_papers:
        args.limit_papers = 1000
        print(f"Quick benchmark mode: limiting to {args.limit_papers} papers")
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'validation_part_{args.part_id:03d}.log')
        ]
    )
    
    # Create and run validation
    validator = EnhancedValidation(
        data_cache_dir=args.data_cache,
        scores_dir=args.scores_dir,
        part_id=args.part_id,
        total_parts=args.total_parts,
        use_tsv=args.use_tsv,
        wos_root=args.wos_root,
        limit_papers=args.limit_papers,
        tsv_vertex_dir=args.tsv_vertex_dir,
        tsv_edge_dir=args.tsv_edge_dir,
        legacy_mode=args.legacy_mode,
        write_cache_dir=args.write_cache_dir,
        reuse_cache_dir=args.reuse_cache_dir,
        validate_cdindex_only=args.validate_cdindex_only,
        max_files=args.max_files,
    )
    
    try:
        validator.run_validation()
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        raise

if __name__ == '__main__':
    main()
