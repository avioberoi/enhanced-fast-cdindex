import pyarrow as pa
import os
import _cdindex

# Re-export CiterFilter enum for convenience
CiterFilter = _cdindex.CiterFilter

class EnhancedGraph:
    def __init__(self):
        self._graph = _cdindex.EnhancedGraph()

    @property
    def properties(self):
        """Access to the PropertyStore for building year bitmaps and indexes."""
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
        return self._graph.vertex_count()

    def edge_count(self):
        return self._graph.edge_count()

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
    
    def build_region_bitmaps(self, us_codes: list, cn_codes: list, eu_codes: list):
        """
        Build region bitmaps for filtering CD-index computations.
        
        Args:
            us_codes: List of country codes representing US
            cn_codes: List of country codes representing China/HK/etc
            eu_codes: List of country codes representing European countries
            
        Note: Country codes should match the integer codes used when ingesting
        country data into the PropertyStore.
        """
        self._graph.build_region_bitmaps(us_codes, cn_codes, eu_codes)
    
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

    def debug_get_citers(self, paper_id: int, years: int) -> list:
        return self._graph.debug_get_citers(paper_id, years)

    def debug_get_references(self, paper_id: int) -> list:
        return self._graph.debug_get_references(paper_id)