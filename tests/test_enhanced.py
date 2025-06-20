import pytest
import pyarrow as pa
from fast_cdindex.cdindex_enhanced import EnhancedGraph
import math
from hypothesis import given, strategies as st, settings
import numpy as np


def make_graph():
    # Build a simple graph: vertices 1,2,3; edges 2->1, 3->1
    ids = [1, 2, 3]
    years = [2000, 2001, 2002]
    vertices_arrow = pa.Table.from_pydict({
        'paper_id': ids,
        'year': years
    })
    # For Graph::add_edge, source must be older than target: so 1->2 and 1->3
    sources = [1, 1]
    targets = [2, 3]
    edges_arrow = pa.Table.from_pydict({
        'source_id': sources,
        'target_id': targets
    })
    g = EnhancedGraph()
    # Use the Python batch ingestion wrappers
    g.add_vertex_batch(vertices_arrow)
    g.add_edge_batch(edges_arrow)
    return g


def make_large_test_graph():
    """Create a larger test graph with meaningful CD-index values"""
    # Create papers with timeline: refs(2000) -> focal(2001) -> citers(2002-2005)
    focal_id = 100
    ref_ids = [1, 2, 3, 4, 5]  # References
    citer_ids = [200, 201, 202, 203, 204, 205, 206, 207, 208, 209]  # Citers
    
    all_ids = ref_ids + [focal_id] + citer_ids
    all_years = [2000] * len(ref_ids) + [2001] + [2002 + i % 4 for i in range(len(citer_ids))]
    
    vertices_arrow = pa.Table.from_pydict({
        'paper_id': all_ids,
        'year': all_years
    })
    
    # Create reference edges: refs -> focal
    ref_sources = ref_ids
    ref_targets = [focal_id] * len(ref_ids)
    
    # Create citation edges: focal -> citers, and some citer cross-references
    cite_sources = [focal_id] * len(citer_ids)
    cite_targets = citer_ids
    
    # Add some cross-references between citers and refs
    cross_sources = citer_ids[:5]  # Some citers also cite the references
    cross_targets = ref_ids
    
    all_sources = ref_sources + cite_sources + cross_sources
    all_targets = ref_targets + cite_targets + cross_targets
    
    edges_arrow = pa.Table.from_pydict({
        'source_id': all_sources,
        'target_id': all_targets
    })
    
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vertices_arrow)
    g.add_edges_from_arrow(edges_arrow)
    
    return g, focal_id, ref_ids, citer_ids


def test_basic_cdindex():
    g = make_graph()
    # No references for focal, so CD index should be 0.0
    assert g.cdindex(1, 10) == pytest.approx(0.0)


def test_filtered_cdindex_no_filter_equals_basic():
    g = make_graph()
    assert g.cdindex_filtered(1, 10, {}) == pytest.approx(g.cdindex(1, 10))


def test_property_filtering():
    g = make_graph()
    # Assign country property: 2->1, 3->2
    prop_table = pa.Table.from_pydict({
        'paper_id': [2, 3],
        'country':  [1, 2]
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    # CD index remains 0.0 regardless of property filter (no references)
    assert g.cdindex_filtered(1, 10, {'country': [1]}) == pytest.approx(0.0)
    assert g.cdindex_filtered(1, 10, {'country': [2]}) == pytest.approx(0.0)
    assert g.cdindex_filtered(1, 10, {'country': [3]}) == pytest.approx(0.0)


def test_cpp_batch_ingest_equivalence():
    # Build identical Arrow tables
    ids = [1, 2, 3]
    years = [2000, 2001, 2002]
    vertices_arrow = pa.Table.from_pydict({
        'paper_id': ids,
        'year': years
    })
    sources = [1, 1]
    targets = [2, 3]
    edges_arrow = pa.Table.from_pydict({
        'source_id': sources,
        'target_id': targets
    })
    # Graph built via Python wrapper loops
    g_loop = EnhancedGraph()
    g_loop.add_vertex_batch(vertices_arrow)
    g_loop.add_edge_batch(edges_arrow)
    # Graph built via C++ batch ingestion
    g_cpp = EnhancedGraph()
    g_cpp.add_vertices_from_arrow(vertices_arrow)
    g_cpp.add_edges_from_arrow(edges_arrow)
    # Compare CD index outputs for each paper
    for pid in ids:
        assert g_cpp.cdindex(pid, 10) == pytest.approx(g_loop.cdindex(pid, 10))


def test_ingest_string_and_bool_properties():
    # Toy graph: focal 1, citers 2 and 3
    g = make_graph()
    # Assign string and boolean properties
    prop_table = pa.Table.from_pydict({
        'paper_id': [2, 3],
        'category': ['A', 'B'],
        'flag':     [True, False]
    })
    # Ingest and build indexes
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    # Ensure no errors and filters work (CD index remains 0)
    # 'A' should map to code 1, 'B' to 2
    score_A = g.cdindex_filtered(1, 10, {'category': [1]})
    score_B = g.cdindex_filtered(1, 10, {'category': [2]})
    score_flag_true  = g.cdindex_filtered(1, 10, {'flag': [1]})
    score_flag_false = g.cdindex_filtered(1, 10, {'flag': [0]})
    assert score_A == pytest.approx(0.0)
    assert score_B == pytest.approx(0.0)
    assert score_flag_true == pytest.approx(0.0)
    assert score_flag_false == pytest.approx(0.0)


def test_cpp_pipeline_filtered_no_filter_after_properties():
    # Toy graph: vertices 1 and 2 reference vertex 3
    ids = [1, 2, 3]
    years = [2000, 2000, 2001]
    vertices_arrow = pa.Table.from_pydict({
        'paper_id': ids,
        'year': years
    })
    sources = [1, 2]
    targets = [3, 3]
    edges_arrow = pa.Table.from_pydict({
        'source_id': sources,
        'target_id': targets
    })
    # Use same year property for ingestion
    prop_arrow = pa.Table.from_pydict({
        'paper_id': ids,
        'year':    years
    })
    # Build and ingest via C++ batch API
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vertices_arrow)
    g.add_edges_from_arrow(edges_arrow)
    g.ingest_properties(prop_arrow)
    g.build_property_indexes()
    # For each paper, cdindex_filtered with empty filters must equal cdindex
    for pid in ids:
        assert g.cdindex_filtered(pid, 1, {}) == pytest.approx(g.cdindex(pid, 1))


def test_property_filtering_effect_on_cdindex():
    # Build a graph where focal paper 1 cites refs 2 and 3, then is cited by 4 and 5
    # Years: refs=2000, focal=2001, citers=2002
    ids = [2, 3, 1, 4, 5]
    years = [2000, 2000, 2001, 2002, 2002]
    vertices_arrow = pa.Table.from_pydict({
        'paper_id': ids,
        'year': years
    })
    # References: 2->1, 3->1
    ref_sources = [2, 3]
    ref_targets = [1, 1]
    # Citations: 1->4, 1->5
    cite_sources = [1, 1]
    cite_targets = [4, 5]
    edges_arrow = pa.Table.from_pydict({
        'source_id': ref_sources + cite_sources,
        'target_id': ref_targets + cite_targets
    })
    # Assign a single property 'group' for citers only: 4->1, 5->2
    prop_arrow = pa.Table.from_pydict({
        'paper_id': [4, 5],
        'group':    [1, 2]
    })
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vertices_arrow)
    g.add_edges_from_arrow(edges_arrow)
    g.ingest_properties(prop_arrow)
    g.build_property_indexes()
    t_delta = 10
    # Unfiltered CD-index for this toy graph should be zero (no valid union within time window)
    baseline_score = g.cdindex(1, t_delta)
    assert math.isclose(baseline_score, 0.0)
    # Any non-empty filter should behave identically (zero)
    f1 = g.cdindex_filtered(1, t_delta, {'group': [1]})
    f2 = g.cdindex_filtered(1, t_delta, {'group': [2]})
    fboth = g.cdindex_filtered(1, t_delta, {'group': [1, 2]})
    assert f1 == pytest.approx(baseline_score)
    assert f2 == pytest.approx(baseline_score)
    assert fboth == pytest.approx(baseline_score)


# ===== NEW COMPREHENSIVE TESTS =====

def test_batch_cdindex_computation():
    """Test batch CD-index computation with cdindex_batch method"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Test single paper batch
    single_paper_array = pa.array([focal_id], type=pa.uint32())
    result_table = g.cdindex_batch(single_paper_array, 5)
    
    assert result_table.num_rows == 1
    assert result_table.schema.names == ['paper_id', 'cd5']
    
    # Compare with single computation
    single_result = g.cdindex(focal_id, 5)
    batch_result = result_table.column('cd5').to_pylist()[0]
    assert batch_result == pytest.approx(single_result)
    
    # Test multiple papers batch
    all_papers = ref_ids + [focal_id] + citer_ids[:3]
    multi_paper_array = pa.array(all_papers, type=pa.uint32())
    result_table = g.cdindex_batch(multi_paper_array, 5)
    
    assert result_table.num_rows == len(all_papers)
    
    # Verify each result matches individual computation
    paper_ids = result_table.column('paper_id').to_pylist()
    cd_scores = result_table.column('cd5').to_pylist()
    
    for pid, batch_score in zip(paper_ids, cd_scores):
        individual_score = g.cdindex(pid, 5)
        assert batch_score == pytest.approx(individual_score)


def test_batch_filtered_cdindex_computation():
    """Test batch filtered CD-index computation"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties for filtering
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [1, 1, 2, 2, 3, 1, 2, 3, 1, 2],  # Mixed journal assignments
        'country': [1, 2, 1, 2, 1, 2, 1, 2, 1, 2]      # Alternating countries
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test filtered batch computation
    test_papers = [focal_id] + ref_ids[:2]
    paper_array = pa.array(test_papers, type=pa.uint32())
    
    # Filter by journal_id=1
    filters = {'journal_id': [1]}
    result_table = g.cdindex_filtered_batch(paper_array, 5, filters)
    
    assert result_table.num_rows == len(test_papers)
    
    # Compare with individual filtered computations
    paper_ids = result_table.column('paper_id').to_pylist()
    cd_scores = result_table.column('cd5').to_pylist()
    
    for pid, batch_score in zip(paper_ids, cd_scores):
        individual_score = g.cdindex_filtered(pid, 5, filters)
        assert batch_score == pytest.approx(individual_score)


def test_multiple_property_filters():
    """Test filtering with multiple properties simultaneously"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add multiple properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [1, 1, 2, 2, 3, 1, 2, 3, 1, 2],
        'country': [1, 2, 1, 2, 1, 2, 1, 2, 1, 2],
        'language': [1, 1, 1, 2, 2, 2, 1, 1, 2, 2]
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test various filter combinations
    filters = [
        {'journal_id': [1]},
        {'country': [1]},
        {'language': [1]},
        {'journal_id': [1], 'country': [1]},
        {'journal_id': [1], 'language': [1]},
        {'country': [1], 'language': [1]},
        {'journal_id': [1], 'country': [1], 'language': [1]},
        {'journal_id': [1, 2], 'country': [1]},
    ]
    
    for filter_dict in filters:
        # Should not crash and should return valid scores
        score = g.cdindex_filtered(focal_id, 5, filter_dict)
        assert isinstance(score, float)
        assert not math.isnan(score)
        assert score >= 0.0  # CD-index should be non-negative


def test_property_filter_edge_cases():
    """Test edge cases in property filtering"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [1, 1, 2, 2, 3, 1, 2, 3, 1, 2],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test empty filter (should equal unfiltered)
    unfiltered = g.cdindex(focal_id, 5)
    empty_filtered = g.cdindex_filtered(focal_id, 5, {})
    assert empty_filtered == pytest.approx(unfiltered)
    
    # Test filter with non-existent property value
    nonexistent_filter = g.cdindex_filtered(focal_id, 5, {'journal_id': [999]})
    assert nonexistent_filter == pytest.approx(0.0)  # Should find no citers
    
    # Test filter with non-existent property name
    invalid_filter = g.cdindex_filtered(focal_id, 5, {'nonexistent_prop': [1]})
    assert invalid_filter == pytest.approx(0.0)  # Should find no citers
    
    # Test filter with empty value list
    empty_values_filter = g.cdindex_filtered(focal_id, 5, {'journal_id': []})
    assert empty_values_filter == pytest.approx(0.0)  # Should find no citers


def test_large_property_dataset():
    """Test with larger property datasets to check performance and correctness"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Create larger property dataset
    extended_ids = citer_ids + list(range(1000, 1100))  # Add 100 more papers
    extended_journals = [i % 10 + 1 for i in range(len(extended_ids))]  # 10 different journals
    extended_countries = [i % 5 + 1 for i in range(len(extended_ids))]  # 5 different countries
    
    prop_table = pa.Table.from_pydict({
        'paper_id': extended_ids,
        'journal_id': extended_journals,
        'country': extended_countries,
    })
    
    # Test chunked ingestion
    g.ingest_properties(prop_table, chunk_size=50)
    g.build_property_indexes()
    
    # Test various filters
    for journal in [1, 5, 10]:
        for country in [1, 3, 5]:
            score = g.cdindex_filtered(focal_id, 5, {
                'journal_id': [journal], 
                'country': [country]
            })
            assert isinstance(score, float)
            assert not math.isnan(score)


def test_time_window_variations():
    """Test CD-index computation with different time windows"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    time_windows = [1, 2, 3, 5, 10, 20]
    scores = []
    
    for window in time_windows:
        score = g.cdindex(focal_id, window)
        scores.append(score)
        assert isinstance(score, float)
        assert not math.isnan(score)
    
    # Scores should generally be non-decreasing with larger time windows
    # (more citers become eligible)
    for i in range(1, len(scores)):
        assert scores[i] >= scores[i-1] - 1e-10  # Allow for small numerical differences


def test_property_cache_management():
    """Test property cache clearing and management"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [1, 1, 2, 2, 3, 1, 2, 3, 1, 2],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Compute filtered score
    score1 = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
    
    # Clear filter cache
    g._graph.clear_filter_cache()
    
    # Compute same score again (should be identical)
    score2 = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
    assert score1 == pytest.approx(score2)
    
    # Clear properties entirely
    g._graph.clear_properties()
    
    # Adding properties again should work
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    score3 = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
    assert score1 == pytest.approx(score3)


def test_empty_graph_edge_cases():
    """Test behavior with empty or minimal graphs"""
    # Empty graph
    g = EnhancedGraph()
    
    # Should handle empty graph gracefully
    score = g.cdindex(1, 5)
    assert score == pytest.approx(0.0)
    
    # Single vertex
    g.add_vertices_from_arrow(pa.Table.from_pydict({
        'paper_id': [1],
        'year': [2000]
    }))
    score = g.cdindex(1, 5)
    assert score == pytest.approx(0.0)
    
    # Test batch operations on empty arrays
    empty_array = pa.array([], type=pa.uint32())
    result = g.cdindex_batch(empty_array, 5)
    assert result.num_rows == 0


def test_invalid_inputs():
    """Test handling of invalid inputs"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Test with non-existent paper ID
    score = g.cdindex(99999, 5)
    assert score == pytest.approx(0.0)
    
    # Test with zero time window
    score = g.cdindex(focal_id, 0)
    assert isinstance(score, float)
    
    # Test with negative time window
    score = g.cdindex(focal_id, -1)
    assert isinstance(score, float)


def test_property_data_types():
    """Test different property data types"""
    g = make_graph()
    
    # Test with different Arrow data types
    prop_table = pa.Table.from_pydict({
        'paper_id': [1, 2, 3],
        'int32_prop': pa.array([1, 2, 3], type=pa.int32()),
        'int64_prop': pa.array([10, 20, 30], type=pa.int64()),
        'bool_prop': pa.array([True, False, True], type=pa.bool_()),
        'string_prop': pa.array(['A', 'B', 'C'], type=pa.string()),
    })
    
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test filtering with each property type
    filters = [
        {'int32_prop': [1]},
        {'int64_prop': [10]},
        {'bool_prop': [1]},  # True maps to 1
        {'string_prop': [1]},  # 'A' maps to 1
    ]
    
    for filter_dict in filters:
        score = g.cdindex_filtered(1, 5, filter_dict)
        assert isinstance(score, float)
        assert not math.isnan(score)


def test_concurrent_operations():
    """Test that operations work correctly when called in sequence"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [1, 1, 2, 2, 3, 1, 2, 3, 1, 2],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Perform multiple operations in sequence
    operations = [
        lambda: g.cdindex(focal_id, 5),
        lambda: g.cdindex_filtered(focal_id, 5, {'journal_id': [1]}),
        lambda: g.cdindex_batch(pa.array([focal_id], type=pa.uint32()), 5),
        lambda: g.cdindex_filtered_batch(pa.array([focal_id], type=pa.uint32()), 5, {'journal_id': [1]}),
    ]
    
    # Run operations multiple times to ensure consistency
    for _ in range(3):
        results = []
        for op in operations:
            result = op()
            results.append(result)
        
        # Basic sanity checks
        assert isinstance(results[0], float)  # cdindex
        assert isinstance(results[1], float)  # cdindex_filtered
        assert isinstance(results[2], pa.Table)  # cdindex_batch
        assert isinstance(results[3], pa.Table)  # cdindex_filtered_batch
        
        # Batch results should match individual results
        assert results[2].column('cd5').to_pylist()[0] == pytest.approx(results[0])
        assert results[3].column('cd5').to_pylist()[0] == pytest.approx(results[1])


# Hypothesis-based property test for random small graphs
@st.composite
def graph_data(draw):
    # Generate small DAG: up to 8 vertices with increasing years
    n = draw(st.integers(min_value=1, max_value=6))
    ids = list(range(1, n + 1))
    years = [2000 + i for i in range(n)]
    # Possible edges (source -> target) only if source < target to ensure acyclicity
    possible = [(ids[i], ids[j]) for i in range(n) for j in range(i + 1, n)]
    # If no possible edges (n<2), return empty edge lists
    if not possible:
        return ids, years, [], []
    edges = draw(st.lists(st.sampled_from(possible), min_size=0, max_size=len(possible), unique=True))
    if edges:
        sources, targets = zip(*edges)
        return ids, years, list(sources), list(targets)
    else:
        return ids, years, [], []

@given(graph_data())
@settings(max_examples=20)
def test_random_batch_equivalence(data):
    ids, years, sources, targets = data
    # Build Arrow tables
    vertices_arrow = pa.Table.from_pydict({'paper_id': ids, 'year': years})
    edges_arrow    = pa.Table.from_pydict({'source_id': sources, 'target_id': targets})
    # Ingest via Python loops
    g_loop = EnhancedGraph()
    g_loop.add_vertex_batch(vertices_arrow)
    g_loop.add_edge_batch(edges_arrow)
    # Ingest via C++ batch
    g_cpp = EnhancedGraph()
    g_cpp.add_vertices_from_arrow(vertices_arrow)
    g_cpp.add_edges_from_arrow(edges_arrow)
    # Compare CD-index outputs on 5-year window
    t_delta = 5
    for pid in ids:
        assert g_loop.cdindex(pid, t_delta) == pytest.approx(g_cpp.cdindex(pid, t_delta))


@given(st.integers(min_value=1, max_value=10))
@settings(max_examples=10)
def test_random_time_windows(time_window):
    """Property test: CD-index with random time windows should be well-behaved"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    score = g.cdindex(focal_id, time_window)
    assert isinstance(score, float)
    assert not math.isnan(score)
    assert score >= 0.0
    assert score <= 1.0  # CD-index should be between 0 and 1


@given(st.lists(st.integers(min_value=1, max_value=5), min_size=1, max_size=10, unique=True))
@settings(max_examples=10) 
def test_random_property_filters(filter_values):
    """Property test: Random property filters should not crash"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'test_prop': [i % 5 + 1 for i in range(len(citer_ids))],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test with random filter values
    score = g.cdindex_filtered(focal_id, 5, {'test_prop': filter_values})
    assert isinstance(score, float)
    assert not math.isnan(score)
    assert score >= 0.0


def test_chunked_property_ingestion():
    """Test property ingestion with different chunk sizes"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Create property data
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [i % 3 + 1 for i in range(len(citer_ids))],
        'country': [i % 2 + 1 for i in range(len(citer_ids))],
    })
    
    # Test different chunk sizes
    chunk_sizes = [1, 3, 5, 10, 20]
    baseline_score = None
    
    for chunk_size in chunk_sizes:
        # Clear previous properties
        g._graph.clear_properties()
        
        # Ingest with specific chunk size
        g.ingest_properties(prop_table, chunk_size=chunk_size)
        g.build_property_indexes()
        
        # Compute filtered score
        score = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
        
        if baseline_score is None:
            baseline_score = score
        else:
            # All chunk sizes should produce identical results
            assert score == pytest.approx(baseline_score), f"Chunk size {chunk_size} produced different result"


def test_filter_cache_behavior():
    """Test filter cache behavior and performance implications"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [i % 5 + 1 for i in range(len(citer_ids))],
        'country': [i % 3 + 1 for i in range(len(citer_ids))],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # First computation should build cache
    filter1 = {'journal_id': [1]}
    score1 = g.cdindex_filtered(focal_id, 5, filter1)
    
    # Second computation with same filter should use cache
    score1_cached = g.cdindex_filtered(focal_id, 5, filter1)
    assert score1 == pytest.approx(score1_cached)
    
    # Different filter should build new cache entry
    filter2 = {'journal_id': [2]}
    score2 = g.cdindex_filtered(focal_id, 5, filter2)
    
    # Combined filter should work correctly
    filter3 = {'journal_id': [1, 2]}
    score3 = g.cdindex_filtered(focal_id, 5, filter3)
    
    # Clear cache and recompute
    g._graph.clear_filter_cache()
    score1_after_clear = g.cdindex_filtered(focal_id, 5, filter1)
    assert score1 == pytest.approx(score1_after_clear)


def test_property_ingestion_edge_cases():
    """Test edge cases in property ingestion"""
    g = make_graph()
    
    # Test ingesting properties for non-existent papers
    prop_table = pa.Table.from_pydict({
        'paper_id': [999, 1000],  # Non-existent papers
        'test_prop': [1, 2],
    })
    
    # Should not crash
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Filters should work but find no matches
    score = g.cdindex_filtered(1, 5, {'test_prop': [1]})
    assert score == pytest.approx(0.0)
    
    # Test ingesting empty properties
    empty_prop_table = pa.Table.from_pydict({
        'paper_id': pa.array([], type=pa.uint32()),
        'empty_prop': pa.array([], type=pa.int32()),
    })
    
    g.ingest_properties(empty_prop_table)
    g.build_property_indexes()


def test_mixed_property_types_comprehensive():
    """Test comprehensive property type handling"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Test all supported property types
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'int8_prop': pa.array([i % 3 for i in range(len(citer_ids))], type=pa.int8()),
        'int16_prop': pa.array([i % 4 for i in range(len(citer_ids))], type=pa.int16()),
        'int32_prop': pa.array([i % 5 for i in range(len(citer_ids))], type=pa.int32()),
        'int64_prop': pa.array([i % 6 for i in range(len(citer_ids))], type=pa.int64()),
        'uint8_prop': pa.array([i % 2 for i in range(len(citer_ids))], type=pa.uint8()),
        'uint16_prop': pa.array([i % 3 for i in range(len(citer_ids))], type=pa.uint16()),
        'uint32_prop': pa.array([i % 4 for i in range(len(citer_ids))], type=pa.uint32()),
        'uint64_prop': pa.array([i % 5 for i in range(len(citer_ids))], type=pa.uint64()),
        'bool_prop': pa.array([i % 2 == 0 for i in range(len(citer_ids))], type=pa.bool_()),
        'string_prop': pa.array([f'cat_{i % 3}' for i in range(len(citer_ids))], type=pa.string()),
    })
    
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test filtering with each property type
    property_filters = [
        {'int8_prop': [0]},
        {'int16_prop': [1]},
        {'int32_prop': [2]},
        {'int64_prop': [3]},
        {'uint8_prop': [0]},
        {'uint16_prop': [1]},
        {'uint32_prop': [2]},
        {'uint64_prop': [3]},
        {'bool_prop': [1]},  # True
        {'bool_prop': [0]},  # False
        {'string_prop': [1]},  # First string maps to 1
    ]
    
    for filter_dict in property_filters:
        score = g.cdindex_filtered(focal_id, 5, filter_dict)
        assert isinstance(score, float)
        assert not math.isnan(score)
        assert score >= 0.0


def test_batch_operations_comprehensive():
    """Test comprehensive batch operations with various scenarios"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties for filtering
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'category': [i % 3 + 1 for i in range(len(citer_ids))],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test batch with single paper
    single_paper = pa.array([focal_id], type=pa.uint32())
    result_single = g.cdindex_batch(single_paper, 5)
    assert result_single.num_rows == 1
    
    # Test batch with multiple papers
    multi_papers = pa.array(ref_ids + [focal_id] + citer_ids[:3], type=pa.uint32())
    result_multi = g.cdindex_batch(multi_papers, 5)
    assert result_multi.num_rows == len(ref_ids) + 1 + 3
    
    # Test filtered batch with single paper
    result_filtered_single = g.cdindex_filtered_batch(single_paper, 5, {'category': [1]})
    assert result_filtered_single.num_rows == 1
    
    # Test filtered batch with multiple papers
    result_filtered_multi = g.cdindex_filtered_batch(multi_papers, 5, {'category': [1, 2]})
    assert result_filtered_multi.num_rows == len(ref_ids) + 1 + 3
    
    # Test batch with duplicate papers
    duplicate_papers = pa.array([focal_id, focal_id, focal_id], type=pa.uint32())
    result_duplicates = g.cdindex_batch(duplicate_papers, 5)
    assert result_duplicates.num_rows == 3
    
    # All results for same paper should be identical
    scores = result_duplicates.column('cd5').to_pylist()
    assert all(s == pytest.approx(scores[0]) for s in scores)


def test_time_window_boundary_conditions():
    """Test CD-index computation at time window boundaries"""
    # Create a graph with precise timing to test boundary conditions
    years = [2000, 2001, 2002, 2003, 2004, 2005, 2006]
    ids = list(range(1, len(years) + 1))
    focal_id = 2  # Paper from 2001
    
    vertices_arrow = pa.Table.from_pydict({
        'paper_id': ids,
        'year': years
    })
    
    # Create references: 1 -> 2
    # Create citations: 2 -> 3, 4, 5, 6, 7
    sources = [1] + [focal_id] * 5
    targets = [focal_id] + list(range(3, 8))
    
    edges_arrow = pa.Table.from_pydict({
        'source_id': sources,
        'target_id': targets
    })
    
    g = EnhancedGraph()
    g.add_vertices_from_arrow(vertices_arrow)
    g.add_edges_from_arrow(edges_arrow)
    
    # Test various time windows that include different numbers of citers
    time_windows = [0, 1, 2, 3, 4, 5, 10]
    scores = []
    
    for window in time_windows:
        score = g.cdindex(focal_id, window)
        scores.append(score)
        assert isinstance(score, float)
        assert not math.isnan(score)
        assert score >= 0.0
    
    # With window=1, only papers from 2002 should be eligible (paper 3)
    # With window=2, papers from 2002-2003 should be eligible (papers 3,4)
    # etc.


def test_property_ingestion_with_null_values():
    """Test property ingestion handling of null/missing values"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Create property table with some null values
    journal_ids = [1, 2, None, 3, None, 1, 2, 3, 1, 2]
    
    # Arrow handles nulls, but let's make it explicit
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': journal_ids,
    })
    
    # Should handle null values gracefully
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Filtering should work (nulls are typically excluded from matches)
    score = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
    assert isinstance(score, float)
    assert not math.isnan(score)


def test_large_filter_combinations():
    """Test filtering with large numbers of filter values"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties with many possible values
    num_journals = 20
    num_countries = 10
    
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [i % num_journals + 1 for i in range(len(citer_ids))],
        'country': [i % num_countries + 1 for i in range(len(citer_ids))],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Test filter with many values
    many_journals = list(range(1, num_journals + 1))
    many_countries = list(range(1, num_countries + 1))
    
    filters = [
        {'journal_id': many_journals},  # All journals
        {'country': many_countries},    # All countries
        {'journal_id': many_journals, 'country': many_countries},  # All combinations
        {'journal_id': many_journals[:10]},  # Half the journals
        {'journal_id': many_journals[:5], 'country': many_countries[:5]},  # Subset combo
    ]
    
    for filter_dict in filters:
        score = g.cdindex_filtered(focal_id, 5, filter_dict)
        assert isinstance(score, float)
        assert not math.isnan(score)
        assert score >= 0.0


def test_graph_state_consistency():
    """Test that graph state remains consistent across operations"""
    g, focal_id, ref_ids, citer_ids = make_large_test_graph()
    
    # Add properties
    prop_table = pa.Table.from_pydict({
        'paper_id': citer_ids,
        'journal_id': [i % 3 + 1 for i in range(len(citer_ids))],
    })
    g.ingest_properties(prop_table)
    g.build_property_indexes()
    
    # Get baseline results by running each operation once
    baseline_cdindex = g.cdindex(focal_id, 5)
    baseline_filtered = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
    baseline_batch = g.cdindex_batch(pa.array([focal_id], type=pa.uint32()), 5)
    baseline_filtered_batch = g.cdindex_filtered_batch(pa.array([focal_id], type=pa.uint32()), 5, {'journal_id': [1]})
    
    # Run operations multiple times to ensure consistency
    for i in range(5):
        # Test cdindex
        score = g.cdindex(focal_id, 5)
        assert score == pytest.approx(baseline_cdindex)
        
        # Test cdindex_filtered
        filtered_score = g.cdindex_filtered(focal_id, 5, {'journal_id': [1]})
        assert filtered_score == pytest.approx(baseline_filtered)
        
        # Test cdindex_batch
        batch_result = g.cdindex_batch(pa.array([focal_id], type=pa.uint32()), 5)
        assert batch_result.column('cd5').to_pylist()[0] == pytest.approx(baseline_cdindex)
        
        # Test cdindex_filtered_batch
        filtered_batch_result = g.cdindex_filtered_batch(pa.array([focal_id], type=pa.uint32()), 5, {'journal_id': [1]})
        assert filtered_batch_result.column('cd5').to_pylist()[0] == pytest.approx(baseline_filtered)
    
    # Verify batch and individual results match
    assert baseline_batch.column('cd5').to_pylist()[0] == pytest.approx(baseline_cdindex)
    assert baseline_filtered_batch.column('cd5').to_pylist()[0] == pytest.approx(baseline_filtered)