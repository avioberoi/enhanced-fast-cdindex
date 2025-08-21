import pyarrow as pa
import pyarrow.parquet as pq
import os
import _cdindex

# Re-export CiterFilter enum and base Graph for convenience
CiterFilter = _cdindex.CiterFilter
Graph = _cdindex.Graph

class EnhancedGraph:
    def __init__(self):
        self._graph = _cdindex.EnhancedGraph()

    @property
    def properties(self):
        """PropertyStore for building/using bitmaps (year, country, etc.)"""
        return self._graph.properties

    def cdindex(self, paper_id: int, years: int):
        """
        Compute the CD index for a paper within a time window.
        
        Args:
            paper_id: The focal paper ID
            years: Time window in years (passed directly to C++)
        """
        return self._graph.cdindex(paper_id, years)
    
    def cdindex_filtered(self, paper_id: int, years: int, filter_type):
        """
        Compute filtered CD index excluding or including specific regions.
        
        Args:
            paper_id: The focal paper ID
            years: Time window in years
            filter_type: CiterFilter enum value (e.g., CiterFilter.ExcludeUS)
            
        Returns:
            Filtered CD index value
        """
        return self._graph.cdindex_filtered(paper_id, years, filter_type)

    def add_vertices_from_arrow(self, arrow_table: pa.Table):
        self._graph.add_vertices_from_arrow(arrow_table)

    def add_edges_from_arrow(self, arrow_table: pa.Table):
        self._graph.add_edges_from_arrow(arrow_table)

    def vertex_count(self):
        return int(self._graph.vertex_count())

    def edge_count(self):
        return int(self._graph.edge_count())

    def iindex(self, paper_id: int, years: int) -> int:
        """
        Compute the I index (in-degree of focal vertex at time t).
        
        Args:
            paper_id: The focal paper ID
            years: Time window in years (passed directly to C++)
            
        Returns:
            Number of papers citing the focal paper within the time window
        """
        return self._graph.iindex(paper_id, years)
    
    def mcdindex(self, paper_id: int, years: int) -> float:
        """
        Compute the mCD index (simplified version: cdindex * iindex).
        
        Args:
            paper_id: The focal paper ID
            years: Time window in years (passed directly to C++)
            
        Returns:
            The mCD index value (cdindex * iindex)
        """
        return self._graph.mcdindex(paper_id, years)
    
    def in_degree(self, paper_id: int) -> int:
        """
        Return the in-degree (number of citing papers) of the focal paper.
        
        Args:
            paper_id: The paper ID
            
        Returns:
            Number of papers citing this paper
        """
        return self._graph.in_degree(paper_id)
    
    def out_degree(self, paper_id: int) -> int:
        """
        Return the out-degree (number of cited papers) of the focal paper.
        
        Args:
            paper_id: The paper ID
            
        Returns:
            Number of papers cited by this paper
        """
        return self._graph.out_degree(paper_id)
    
    def in_edges(self, paper_id: int) -> list:
        """
        Return the list of papers citing the focal paper.
        
        Args:
            paper_id: The paper ID
            
        Returns:
            List of paper IDs that cite this paper
        """
        return self._graph.in_edges(paper_id)
    
    def out_edges(self, paper_id: int) -> list:
        """
        Return the list of papers cited by the focal paper.
        
        Args:
            paper_id: The paper ID
            
        Returns:
            List of paper IDs cited by this paper
        """
        return self._graph.out_edges(paper_id)
    
    def get_timestamp(self, paper_id: int) -> int:
        """
        Return the timestamp (publication year) of the paper.
        
        Args:
            paper_id: The paper ID
            
        Returns:
            The publication year of the paper
        """
        return self._graph.get_timestamp(paper_id)
    
    def clear_predecessor_cache(self):
        """Clear the predecessor bitmap cache."""
        self._graph.clear_predecessor_cache()
    
    def prepare_for_searching(self):
        """Prepare graph for efficient searching by sorting edges."""
        self._graph.prepare_for_searching()
    
    def build_region_bitmaps(self, us_names: list, cn_names: list, eu_names: list):
        """
        Build region bitmaps for filtering CD-index computations (by COUNTRY NAMES)
        
        Note: Names must match normalized lowercase strings in parquet
        
        Args:
            us_names: list[str] names for US (e.g. ["usa","united states"])
            cn_names: list[str] names for CN/HK/Macau variants
            eu_names: list[str] names for EU countries
            
        Note: Names must match normalized lowercase strings in parquet
        """
        self._graph.build_region_bitmaps(us_names, cn_names, eu_names)
    
    def ingest_countries_from_parquet(self, table: pa.Table, uid_col: str = "UID", country_col: str = "country"):
        """
        Ingest normalized country strings and build 'country' bitmaps directly.
        """
        self._graph.ingest_countries_from_parquet(table, uid_col, country_col)
    
    # Micro-benchmarking interface
    def reset_benchmark(self):
        """Reset micro-benchmark counters."""
        _cdindex.g_benchmark.reset()
    
    def print_benchmark_summary(self):
        """Print detailed micro-benchmark results."""
        _cdindex.g_benchmark.print_summary()
    
    def get_benchmark_stats(self) -> dict:
        """Get micro-benchmark statistics as a dictionary."""
        bench = _cdindex.g_benchmark
        total_time = bench.t1_ms + bench.t2_ms + bench.t3_ms
        return {
            'computations': bench.computations,
            't1_ms': bench.t1_ms,  # F_t build time
            't2_ms': bench.t2_ms,  # B_t build time  
            't3_ms': bench.t3_ms,  # Cardinality ops time
            'total_ms': total_time,
            'throughput_per_sec': bench.computations / (total_time / 1000.0) if total_time > 0 else 0,
            'hf_hit_rate': bench.hf_hit / bench.hf_all if bench.hf_all > 0 else 0,
            'hb_hit_rate': bench.hb_hit / bench.hb_all if bench.hb_all > 0 else 0,
            'hu_hit_rate': bench.hu_hit / bench.hu_all if bench.hu_all > 0 else 0,
        }

    # def debug_get_citers(self, paper_id: int, years: int) -> list:
    #     return self._graph.debug_get_citers(paper_id, years)

    # def debug_get_references(self, paper_id: int) -> list:
    #     return self._graph.debug_get_references(paper_id)


    from typing import Optional

    def read_graph_from_tsv_cache(tsv_cache_dir: str,
                                  load_properties: bool = True,
                                  limit_edges: Optional[int] = None):
        """
        Load a graph from TSV cache files.
        
        Args:
            tsv_cache_dir: Directory containing paper_years.parquet (or vertices.parquet) and edges.parquet
            load_properties: Whether to load and index properties (for year filtering)
            limit_edges: Optional limit on number of edges to load (for testing)
            
        Returns:
            EnhancedGraph instance with data loaded
        """
        graph = EnhancedGraph()
        
        # Load vertices
        vertices_path = os.path.join(tsv_cache_dir, 'paper_years.parquet')
        if not os.path.exists(vertices_path):
            alt_vertices = os.path.join(tsv_cache_dir, 'vertices.parquet')
            if os.path.exists(alt_vertices):
                vertices_path = alt_vertices
        if os.path.exists(vertices_path):
            vertices_table = pq.read_table(vertices_path)
            graph.add_vertices_from_arrow(vertices_table)
            
            # Load properties if requested
            if load_properties:
                graph.properties.ingest_arrow(vertices_table)
                graph.properties.build_indexes()
        
        # Load edges
        edges_path = os.path.join(tsv_cache_dir, 'edges.parquet')
        if os.path.exists(edges_path):
            if isinstance(limit_edges, int) and limit_edges > 0:
                # Read limited edges for testing
                edges_table = pq.read_table(edges_path)
                if edges_table.num_rows > limit_edges:
                    edges_table = edges_table.slice(0, limit_edges)
            else:
                edges_table = pq.read_table(edges_path)
            graph.add_edges_from_arrow(edges_table)
        
        # Prepare for searching
        graph.prepare_for_searching()
        
        return graph