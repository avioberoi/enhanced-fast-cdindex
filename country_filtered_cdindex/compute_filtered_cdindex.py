import os
import sys
import pyarrow.parquet as pq
import numpy as np
import argparse
import time
import pandas as pd

# Add the parent directory to import the enhanced graph
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from fast_cdindex.cdindex_enhanced import EnhancedGraph, CiterFilter

def load_graph_fast(cache_dir: str) -> EnhancedGraph:
    print(f"Loading cache from {cache_dir}")
    
    # Load all tables in one batch
    vertices_table = pq.read_table(f"{cache_dir}/paper_years.parquet")
    edges_table = pq.read_table(f"{cache_dir}/edges.parquet") 
    props_table = pq.read_table(f"{cache_dir}/combined_properties.parquet")
    
    print(f"Tables: {vertices_table.num_rows:,} vertices, {edges_table.num_rows:,} edges")
    
    # Single optimized graph construction
    graph = EnhancedGraph()
    graph.add_vertices_from_arrow(vertices_table)
    graph.add_edges_from_arrow(edges_table)
    graph.prepare_for_searching()
    graph.properties.ingest_arrow(props_table)
    graph.properties.build_indexes()
    # Build region bitmaps for filtering (using our country code mapping)
    # US: 1000, China: 2000, EU: 3000 (from PropertyStore normalization)
    graph.build_region_bitmaps([1000], [2000], [3000])
    
    print(f"Graph ready: {graph.vertex_count():,} vertices, {graph.edge_count():,} edges")
    return graph

def compute_filtered(graph: EnhancedGraph, 
                               paper_ids: np.ndarray, 
                               time_delta: int,
                               filters: list) -> np.ndarray:
    """ULTRA-OPTIMIZED: Zero-overhead computation using NumPy arrays"""
    
    n_papers = len(paper_ids)
    n_filters = len(filters)
    
    # Pre-allocate NumPy array for maximum speed (no Python object creation)
    results = np.full((n_papers, n_filters), np.nan, dtype=np.float64)
    
    # Single C++ call per paper (minimize Python/C++ boundary crossings)
    for i in range(n_papers):
        paper_id = int(paper_ids[i])  # Single type conversion
        
        # Inner loop vectorized to C++ calls (no Python exception handling)
        for j in range(n_filters):
            results[i, j] = graph.cdindex_filtered(paper_id, time_delta, filters[j])
    
    return results

def save_results(paper_ids: np.ndarray, results: np.ndarray, 
                            filter_names: list, output_file: str):
    # Convert NumPy arrays directly to DataFrame (no Python loops)
    n_papers, n_filters = results.shape
    
    # Create DataFrame directly from NumPy arrays (fastest method)
    data = {'paper_id': paper_ids}
    for j, filter_name in enumerate(filter_names):
        data[f'cdindex_{filter_name}'] = results[:, j]
    
    df = pd.DataFrame(data)
    
    # Save with optimal compression
    if output_file.endswith('.gz'):
        df.to_csv(output_file, index=False, compression='gzip')
    else:
        df.to_csv(output_file, index=False)
    
    print(f"Saved {len(df):,} papers to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Filtered CD-Index Computation')
    parser.add_argument('--cache-dir', required=True, help='Augmented cache directory')
    parser.add_argument('--output-file', required=True, help='Output CSV file') 
    parser.add_argument('--sample-papers', type=int, help='Sample N random papers')
    parser.add_argument('--paper-ids-file', help='File with paper IDs (one per line)')
    parser.add_argument('--time-delta', type=int, default=5, help='Time window (default: 5)')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Batch size (default: 10000)')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    # Load graph
    graph = load_graph_fast(args.cache_dir)
    
    # Determine paper IDs as NumPy array (fastest data structure)
    if args.paper_ids_file:
        print(f"Loading paper IDs from {args.paper_ids_file}")
        paper_ids = np.loadtxt(args.paper_ids_file, dtype=np.uint32)
    elif args.sample_papers:
        print(f"Sampling {args.sample_papers} random papers")
        max_vertex = graph.vertex_count() - 1
        paper_ids = np.random.randint(0, max_vertex, args.sample_papers, dtype=np.uint32)
    else:
        raise ValueError("Must specify --paper-ids-file or --sample-papers")
    
    print(f"Computing filtered CD-index for {len(paper_ids):,} papers")
    
    # All filters
    filters = [
        CiterFilter.OnlyUS, CiterFilter.OnlyCN, CiterFilter.OnlyEU,
        CiterFilter.ExcludeUS, CiterFilter.ExcludeCN, CiterFilter.ExcludeEU
    ]
    filter_names = [f.name.lower() for f in filters]
    
    print("Starting computation...")
    results = compute_filtered(graph, paper_ids, args.time_delta, filters)
    
    save_results(paper_ids, results, filter_names, args.output_file)
    
    # Summary
    elapsed = time.time() - start_time
    rate = len(paper_ids) / elapsed
    total_computations = len(paper_ids) * len(filters)
    
    print(f"\n=== SUMMARY ===")
    print(f"Papers processed: {len(paper_ids):,}")
    print(f"Total computations: {total_computations:,}")
    print(f"Processing time: {elapsed:.1f}s")
    print(f"Rate: {rate:.1f} papers/sec")
    print(f"C++ computation rate: {total_computations/elapsed:.0f} ops/sec")

if __name__ == "__main__":
    main()
