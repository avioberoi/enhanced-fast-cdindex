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