import pyarrow as pa
import os
import _cdindex

class EnhancedGraph:
    def __init__(self):
        self._graph = _cdindex.EnhancedGraph()

    def add_vertex_batch(self, arrow_table: pa.Table):
        ids = arrow_table.column('paper_id').to_pylist()
        years = arrow_table.column('year').to_pylist()
        for pid, year in zip(ids, years):
            self._graph.add_vertex(pid, int(year))

    def add_edge_batch(self, arrow_table: pa.Table):
        sources = arrow_table.column('source_id').to_pylist()
        targets = arrow_table.column('target_id').to_pylist()
        for src, tgt in zip(sources, targets):
            self._graph.add_edge(src, tgt)

    def ingest_properties(self, arrow_table: pa.Table, chunk_size: int = None):
        old_cs = None
        if chunk_size is not None:
            old_cs = os.environ.get('INGEST_CHUNK_SIZE')
            os.environ['INGEST_CHUNK_SIZE'] = str(chunk_size)
        try:
            self._graph.ingest_properties(arrow_table)
        finally:
            if chunk_size is not None:
                if old_cs is None:
                    del os.environ['INGEST_CHUNK_SIZE']
                else:
                    os.environ['INGEST_CHUNK_SIZE'] = old_cs

    def build_property_indexes(self):
        self._graph.build_property_indexes()

    def cdindex(self, paper_id: int, years: int):
        return self._graph.cdindex(paper_id, years)

    def cdindex_filtered(self, paper_id: int, years: int, filters: dict):
        return self._graph.cdindex_filtered(paper_id, years, filters)

    def cdindex_batch(self, array: pa.Array, years: int) -> pa.Table:
        return self._graph.cdindex_batch(array, years)

    def cdindex_filtered_batch(self, array: pa.Array, years: int, filters: dict) -> pa.Table:
        return self._graph.cdindex_filtered_batch(array, years, filters)

    def clear_filter_cache(self):
        """Clear the filter bitmap cache."""
        self._graph.clear_filter_cache()

    def cdindex_smart(self, paper_ids, years: int, filters: dict = None) -> pa.Table:
        """
        Smart dispatch that automatically chooses between single and batch calls
        based on request size and filter complexity.
        
        Args:
            paper_ids: int, list of ints, or PyArrow Array of paper IDs
            years: time window in years
            filters: optional filter dictionary
            
        Returns:
            PyArrow Table with paper_id and cd{years} columns
        """
        # Handle single ID case
        if isinstance(paper_ids, int):
            paper_ids = [paper_ids]
            
        # Convert to PyArrow array if needed
        if isinstance(paper_ids, list):
            paper_ids = pa.array(paper_ids, type=pa.uint32())
            
        request_count = len(paper_ids)
        has_filters = filters is not None and len(filters) > 0
        
        # Dispatch decision based on micro-benchmark results
        if not has_filters:
            # Unfiltered: single calls are much faster (350,000+ pps vs 6,000 pps batch even with optimizations)
            # ALWAYS prefer single calls for unfiltered requests (batch never wins due to Arrow overhead)
            if request_count <= 10000:  # Raised threshold - single is always better for unfiltered
                # Use individual calls for all practical unfiltered batches
                results = []
                for i in range(request_count):
                    pid = paper_ids[i].as_py()
                    score = self.cdindex(pid, years)
                    results.append({'paper_id': pid, f'cd{years}': score})
                
                # Convert to PyArrow Table
                import pandas as pd
                df = pd.DataFrame(results)
                return pa.Table.from_pandas(df, preserve_index=False)
            else:
                # Use batch call only for extremely large unfiltered requests (>10K papers)
                return self.cdindex_batch(paper_ids, years)
        else:
            # Filtered: batch calls are optimal (5,000+ pps vs 0.5 pps single)
            # Always prefer batch for any multi-paper filtered request
            if request_count == 1:
                # Single filtered computation only for exactly 1 paper
                pid = paper_ids[0].as_py()
                score = self.cdindex_filtered(pid, years, filters)
                results = [{'paper_id': pid, f'cd{years}': score}]
                
                import pandas as pd
                df = pd.DataFrame(results)
                return pa.Table.from_pandas(df, preserve_index=False)
            else:
                # Use batch call for all multi-paper filtered requests (even as small as 2 papers)
                return self.cdindex_filtered_batch(paper_ids, years, filters)

    def add_vertices_from_arrow(self, arrow_table: pa.Table):
        self._graph.add_vertices_from_arrow(arrow_table)

    def add_edges_from_arrow(self, arrow_table: pa.Table):
        self._graph.add_edges_from_arrow(arrow_table)

    def vertex_count(self):
        return self._graph.vertex_count()

    def edge_count(self):
        return self._graph.edge_count()

    def debug_get_citers(self, paper_id: int, years: int) -> list:
        return self._graph.debug_get_citers(paper_id, years)

    def debug_get_references(self, paper_id: int) -> list:
        return self._graph.debug_get_references(paper_id)