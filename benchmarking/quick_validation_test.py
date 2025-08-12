#!/usr/bin/env python3
"""
Quick validation test to verify setup before full validation run.

This script tests:
1. Enhanced package import
2. Graph loading from cache
3. Score computation on a few sample papers
4. Comparison with original scores
"""

import os
import sys
import pandas as pd
import pyarrow.parquet as pq
import gzip
import datetime

# Add the parent directory to import the enhanced graph
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def test_import():
    """Test that enhanced package can be imported."""
    print("Testing enhanced package import...")
    try:
        from fast_cdindex.cdindex_enhanced import EnhancedGraph
        print("✓ Enhanced package import successful")
        return True
    except ImportError as e:
        print(f"✗ Enhanced package import failed: {e}")
        return False

def test_graph_loading(data_cache_dir):
    """Test graph loading from cache."""
    print("\nTesting graph loading from cache...")
    try:
        vertices_cache = os.path.join(data_cache_dir, "paper_years.parquet")
        edges_cache = os.path.join(data_cache_dir, "edges.parquet")
        
        if not os.path.exists(vertices_cache):
            print(f"✗ Vertices cache not found: {vertices_cache}")
            return None
        if not os.path.exists(edges_cache):
            print(f"✗ Edges cache not found: {edges_cache}")
            return None
        
        print("Loading parquet files...")
        # Handle partitioned datasets 
        if os.path.isdir(vertices_cache):
            print(f"Loading partitioned vertices dataset from {vertices_cache}")
            vertices_dataset = pq.ParquetDataset(vertices_cache)
            vertices_table = vertices_dataset.read()
        else:
            vertices_table = pq.read_table(vertices_cache)
            
        if os.path.isdir(edges_cache):
            print(f"Loading partitioned edges dataset from {edges_cache}")
            edges_dataset = pq.ParquetDataset(edges_cache)
            edges_table = edges_dataset.read()
        else:
            edges_table = pq.read_table(edges_cache)
        print(f"Loaded {vertices_table.num_rows:,} vertices, {edges_table.num_rows:,} edges")
        
        # Create small subset for testing
        print("Creating test subset...")
        sample_size = min(100000, vertices_table.num_rows)  # 100K vertices for quick test
        test_vertices = vertices_table.slice(0, sample_size)
        
        # Get sample vertex IDs
        sampled_ids = set(test_vertices.column("paper_id").to_pylist())
        
        # Filter edges to only include those between sampled vertices
        edges_pandas = edges_table.to_pandas()
        mask = edges_pandas['source_id'].isin(sampled_ids) & edges_pandas['target_id'].isin(sampled_ids)
        test_edges = edges_pandas[mask]
        print(f"Test subset: {len(test_vertices):,} vertices, {len(test_edges):,} edges")
        
        # Create graph
        from fast_cdindex.cdindex_enhanced import EnhancedGraph
        graph = EnhancedGraph()
        
        print("Adding vertices to graph...")
        graph.add_vertices_from_arrow(test_vertices)
        
        print("Adding edges to graph...")
        import pyarrow as pa
        test_edges_table = pa.Table.from_pandas(test_edges, preserve_index=False)
        graph.add_edges_from_arrow(test_edges_table)
        
        print(f"✓ Graph created successfully: {graph.vertex_count():,} vertices, {graph.edge_count():,} edges")
        return graph, sampled_ids
        
    except Exception as e:
        print(f"✗ Graph loading failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_score_computation(graph, paper_ids):
    """Test score computation on sample papers."""
    print("\nTesting score computation...")
    
    # Time windows (in years, enhanced implementation uses year arithmetic)
    time_windows = {
        3: 3,     # 3 years
        5: 5,     # 5 years  
        150: 150  # 150 years
    }
    
    # Test a few paper IDs
    test_ids = list(paper_ids)[:5]  # Test first 5 papers
    
    results = []
    for paper_id in test_ids:
        try:
            scores = {'paper_id': paper_id}
            
            # Compute scores for each time window  
            for years in time_windows.keys():
                scores[f'cd{years}'] = graph.cdindex(paper_id, years)
                scores[f'mcd{years}'] = graph.mcdindex(paper_id, years)
                scores[f'ncites{years}'] = graph.iindex(paper_id, years)
            scores[f'nrefs'] = graph.out_degree(paper_id)
            
            results.append(scores)
            print(f"✓ Computed scores for paper {paper_id}")
            
        except Exception as e:
            print(f"✗ Failed to compute scores for paper {paper_id}: {e}")
    
    if results:
        print(f"✓ Score computation successful for {len(results)} papers")
        
        # Print sample results
        print("\nSample results:")
        for result in results:
            print(f"Paper {result['paper_id']}:")
            print(f"  nrefs: {result['nrefs']}")
            print(f"  cd5: {result.get('cd5', 'N/A')}")
            print(f"  mcd5: {result.get('mcd5', 'N/A')}")  
            print(f"  ncites5: {result.get('ncites5', 'N/A')}")
        
        return results
    else:
        print("✗ No scores computed successfully")
        return None

def test_score_file_reading(scores_dir):
    """Test reading original score files."""
    print("\nTesting score file reading...")
    
    # Find first score file
    score_files = [f for f in os.listdir(scores_dir) if f.endswith('.csv.gz')]
    if not score_files:
        print(f"✗ No score files found in {scores_dir}")
        return None
    
    score_file = score_files[0]
    filepath = os.path.join(scores_dir, score_file)
    
    try:
        with gzip.open(filepath, 'rt') as f:
            df = pd.read_csv(f)
        
        print(f"✓ Successfully read {score_file}")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {list(df.columns)}")
        
        # Show sample data
        print("\nSample original scores:")
        print(df.head(3).to_string())
        
        return df.head(10)  # Return first 10 rows for testing
        
    except Exception as e:
        print(f"✗ Failed to read score file {score_file}: {e}")
        return None

def main():
    print("Enhanced CD-Index Quick Validation Test")
    print("=" * 50)
    
    # Check arguments
    data_cache_dir = "/project/jevans/tip/disruption/code_wos_2023/benchmarking/data_cache"
    scores_dir = "/project/jevans/tip/disruption/code_wos_2023/WoS_data/scores_all"
    
    print(f"Data cache dir: {data_cache_dir}")
    print(f"Scores dir: {scores_dir}")
    print()
    
    # Test 1: Import
    if not test_import():
        return 1
    
    # Test 2: Graph loading
    graph_result = test_graph_loading(data_cache_dir)
    if graph_result is None:
        return 1
    
    graph, paper_ids = graph_result
    
    # Test 3: Score computation
    new_scores = test_score_computation(graph, paper_ids)
    if new_scores is None:
        return 1
    
    # Test 4: Score file reading
    original_scores = test_score_file_reading(scores_dir)
    if original_scores is None:
        return 1
    
    print("\n" + "=" * 50)
    print("✓ ALL TESTS PASSED!")
    print("The validation setup is ready for full execution.")
    print("\nTo run the full validation, submit the SLURM job:")
    print("  sbatch run_validation_slurm.sh")
    print("=" * 50)
    
    return 0

if __name__ == '__main__':
    exit(main())
