#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <arrow/python/pyarrow.h>
#include "cdindex_enhanced.h"

namespace py = pybind11;

PYBIND11_MODULE(_cdindex, m) {
    py::module_::import("pyarrow");
    arrow::py::import_pyarrow();
    
    // Expose CiterFilter enum for region filtering
    py::enum_<CiterFilter>(m, "CiterFilter")
        .value("None", CiterFilter::None)
        .value("ExcludeUS", CiterFilter::ExcludeUS)
        .value("ExcludeCN", CiterFilter::ExcludeCN)
        .value("ExcludeEU", CiterFilter::ExcludeEU)
        .value("OnlyUS", CiterFilter::OnlyUS)
        .value("OnlyCN", CiterFilter::OnlyCN)
        .value("OnlyEU", CiterFilter::OnlyEU);

    // Base Graph class (for testing/compatibility)
    py::class_<Graph>(m, "Graph")
        .def(py::init<>())
        .def("add_vertex", &Graph::add_vertex)
        .def("add_edge", &Graph::add_edge)
        .def("cdindex", &Graph::cdindex, py::arg("paper_id"), py::arg("years"))
        .def("cdindex_filtered", &Graph::cdindex_filtered, 
             py::arg("paper_id"), py::arg("years"), py::arg("filter"),
             "Compute filtered CD-index excluding or including specific regions")
        .def("iindex", &Graph::iindex, py::arg("paper_id"), py::arg("years"))
        .def("mcdindex", &Graph::mcdindex, py::arg("paper_id"), py::arg("years"))
        .def("in_degree", &Graph::in_degree, py::arg("paper_id"))
        .def("out_degree", &Graph::out_degree, py::arg("paper_id"))
        .def("in_edges", &Graph::in_edges, py::arg("paper_id"))
        .def("out_edges", &Graph::out_edges, py::arg("paper_id"))
        .def("get_timestamp", &Graph::get_timestamp, py::arg("paper_id"))
        .def("vertex_count", &Graph::vertex_count)
        .def("edge_count", &Graph::edge_count)
        .def("prepare_for_searching", &Graph::prepare_for_searching)
        .def("get_citers", &Graph::get_citers, py::arg("paper_id"), py::arg("years"));
    
    py::class_<PropertyStore>(m, "PropertyStore")
        .def("ingest_arrow", [](PropertyStore &self, py::object table_obj) {
            PyObject* pobj = table_obj.ptr();
            auto maybe_table = arrow::py::unwrap_table(pobj);
            if (!maybe_table.ok()) throw std::runtime_error(maybe_table.status().message());
            auto status = self.ingest_arrow(*maybe_table);
            if (!status.ok()) throw std::runtime_error(status.message());
        })
        .def("build_indexes", &PropertyStore::build_indexes)
        .def("clear", &PropertyStore::clear)
        .def("save_year_bitmaps", [](PropertyStore& self, const std::string& dir) {
            if (!self.save_bitmaps("year", dir)) throw std::runtime_error("save_year_bitmaps failed");
        })
        .def("load_year_bitmaps", [](PropertyStore& self, const std::string& dir) {
            if (!self.load_bitmaps("year", dir)) throw std::runtime_error("load_year_bitmaps failed");
        });

    // EnhancedGraph inherits from Graph
    py::class_<EnhancedGraph, Graph>(m, "EnhancedGraph")
        .def(py::init<>())
        .def("abi_cookie", &EnhancedGraph::cdindex_abi_cookie)
        .def("add_vertex", &EnhancedGraph::add_vertex)
        .def("add_edge", &EnhancedGraph::add_edge)
        .def("cdindex", &EnhancedGraph::cdindex, py::arg("paper_id"), py::arg("years"))
        .def("cdindex_filtered", &EnhancedGraph::cdindex_filtered, 
             py::arg("paper_id"), py::arg("years"), py::arg("filter"),
             "Compute filtered CD-index excluding or including specific regions")

        .def("cdindex_all",
            [](EnhancedGraph& self, VertexId fid, time_delta_t dt) {
            std::array<double,7> a;
            {
                py::gil_scoped_release release;       // compute without GIL
                a = self.cdindex_all(fid, dt);
            }   // GIL reacquired here, before we access a[...]
            return py::make_tuple(a[0], a[1], a[2],
                                    a[3], a[4], a[5], a[6]);
            },
            py::arg("paper_id"), py::arg("years"),
            "Compute base CD and 6 regional variants in a single pass")
        
        .def("clear_predecessor_cache", &EnhancedGraph::clear_predecessor_cache)
        .def("verify_incoming_integrity", &EnhancedGraph::verify_incoming_integrity, 
             py::arg("samples") = 1000, "Verify pointer integrity in incoming_edges")
        .def("verify_regions_against_years", &EnhancedGraph::verify_regions_against_years,
             py::arg("ft"), py::arg("dt"), "Verify region bitmaps work with time windows")
        .def_readonly("properties", &EnhancedGraph::properties)
        .def("add_vertices_from_arrow", [](EnhancedGraph &self, py::object table_obj) {
            PyObject* pobj = table_obj.ptr();
            auto maybe_table = arrow::py::unwrap_table(pobj);
            if (!maybe_table.ok()) throw std::runtime_error(maybe_table.status().message());
            self.add_vertices_from_arrow(*maybe_table);
        })
        .def("add_edges_from_arrow", [](EnhancedGraph &self, py::object table_obj) {
            PyObject* pobj = table_obj.ptr();
            auto maybe_table = arrow::py::unwrap_table(pobj);
            if (!maybe_table.ok()) throw std::runtime_error(maybe_table.status().message());
            self.add_edges_from_arrow(*maybe_table);
        })
        .def("vertex_count", &EnhancedGraph::vertex_count)
        .def("edge_count", &EnhancedGraph::edge_count)
        .def("iindex", &EnhancedGraph::iindex, py::arg("paper_id"), py::arg("years"))
        .def("mcdindex", &EnhancedGraph::mcdindex, py::arg("paper_id"), py::arg("years"))
        .def("in_degree", &EnhancedGraph::in_degree, py::arg("paper_id"))
        .def("out_degree", &EnhancedGraph::out_degree, py::arg("paper_id"))
        .def("in_edges", &EnhancedGraph::in_edges, py::arg("paper_id"))
        .def("out_edges", &EnhancedGraph::out_edges, py::arg("paper_id"))
        .def("get_timestamp", &EnhancedGraph::get_timestamp, py::arg("paper_id"))
        .def("prepare_for_searching", &EnhancedGraph::prepare_for_searching)

        // region build (names) and persistence
        .def("set_country_lists", [](EnhancedGraph& self,
                                     const std::vector<std::string>& us_names,
                                     const std::vector<std::string>& cn_names,
                                     const std::vector<std::string>& eu_names) {
            self.set_country_lists_by_names(us_names, cn_names, eu_names);
        }, py::arg("us_names"), py::arg("cn_names"), py::arg("eu_names"))

        // .def("ingest_countries_from_parquet", [](EnhancedGraph& self,
        //                                          py::object table_obj,
        //                                          const std::string& uid_col,
        //                                          const std::string& country_col) {
        //     PyObject* pobj = table_obj.ptr();
        //     auto maybe_table = arrow::py::unwrap_table(pobj);
        //     if (!maybe_table.ok()) throw std::runtime_error(maybe_table.status().message());
        //     auto st = self.ingest_countries_from_parquet(*maybe_table, uid_col, country_col);
        //     if (!st.ok()) throw std::runtime_error(st.message());
        // }, py::arg("table"), py::arg("uid_col")="UID", py::arg("country_col")="country")

        .def("save_region_bitmaps", [](EnhancedGraph& self, const std::string& dir) {
            if (!self.save_region_bitmaps(dir)) throw std::runtime_error("save_region_bitmaps failed");
        })

        .def("load_region_bitmaps", [](EnhancedGraph& self, const std::string& dir) {
            if (!self.load_region_bitmaps(dir)) throw std::runtime_error("load_region_bitmaps failed");
        })

        // optional: year bitmaps persistence at graph-level (delegates to PropertyStore)
        .def("save_year_bitmaps", [](EnhancedGraph& self, const std::string& dir) {
            if (!self.properties.save_bitmaps("year", dir)) throw std::runtime_error("save_year_bitmaps failed");
        })

        .def("load_year_bitmaps", [](EnhancedGraph& self, const std::string& dir) {
            if (!self.properties.load_bitmaps("year", dir)) throw std::runtime_error("load_year_bitmaps failed");
        })

        // Build region bitmaps from COUNTRY NAMES (strings), which matches the C++ impl
        // .def("build_region_bitmaps", [](EnhancedGraph &self,
        //                                 const std::vector<std::string>& us_names,
        //                                 const std::vector<std::string>& cn_names,
        //                                 const std::vector<std::string>& eu_names) {
        //     EnhancedGraph::CountryLists lists;
        //     lists.us_names = us_names;
        //     lists.cn_names = cn_names;
        //     lists.eu_names = eu_names;
        //     self.build_region_bitmaps_from(self.properties, lists);
        // }, py::arg("us_names"), py::arg("cn_names"), py::arg("eu_names"),
        // "Build region bitmaps from country NAMES for filtering (strings, lowercase).")

        // Direct ingest of countries parquet (UID -> country strings)
        .def("ingest_countries_from_parquet",
             [](EnhancedGraph &self, py::object table_obj,
                const std::string& uid_col, const std::string& country_col) {
                 PyObject* pobj = table_obj.ptr();
                 auto maybe_table = arrow::py::unwrap_table(pobj);
                 if (!maybe_table.ok()) throw std::runtime_error(maybe_table.status().message());
                 auto st = self.ingest_countries_from_parquet(*maybe_table, uid_col, country_col);
                 if (!st.ok()) throw std::runtime_error(st.message());
             },
             py::arg("table"),
             py::arg("uid_col") = "UID",
             py::arg("country_col") = "country",
             "Ingest normalized country strings and build 'country' bitmaps directly.")
        
        .def("clear_uid_map", &EnhancedGraph::clear_uid_map, "Release UID→id map and pinned Arrow UID chunks")
        .def("set_flip_edge_direction_on_ingest", &EnhancedGraph::set_flip_edge_direction_on_ingest,
                 "Interpret input edges as (cited,citing) and flip to (citing,cited) at ingest")
        .def("debug_check_bany", &EnhancedGraph::debug_check_bany,
                "Cheap guardrail: compare cached B_any(fid) with fallback rebuild; returns True on match")
        .def("debug_counts", &EnhancedGraph::debug_counts,
            py::arg("paper_id"), py::arg("years"),
            "Return (|F_t|, |F_t∩B_any|, |B_any∩W_t|, denom, cd) for sanity checks")
            // cheap guardrail: B_any cardinality for focal (builds/uses cache)
        .def("bany_cardinality", [](EnhancedGraph& g, VertexId fid){
            auto sp = g.get_bany_for_focal_sp(fid);
            return static_cast<uint64_t>(sp ? sp->cardinality() : 0);
            }, py::arg("paper_id"),
            "Cardinality of B_any for focal (union of citers of its references)")
        .def("region_sizes", &EnhancedGraph::region_sizes);
    
    // Micro-benchmarking interface
    py::class_<CDIndexBenchmark>(m, "CDIndexBenchmark")
        .def_readonly("computations", &CDIndexBenchmark::n)
        .def_readonly("t1_ms", &CDIndexBenchmark::t1)
        .def_readonly("t2_ms", &CDIndexBenchmark::t2)
        .def_readonly("t3_ms", &CDIndexBenchmark::t3)
        .def_readonly("hf_hit", &CDIndexBenchmark::hf_hit)
        .def_readonly("hf_all", &CDIndexBenchmark::hf_all)
        .def_readonly("hb_hit", &CDIndexBenchmark::hb_hit)
        .def_readonly("hb_all", &CDIndexBenchmark::hb_all)
        .def_readonly("hu_hit", &CDIndexBenchmark::hu_hit)
        .def_readonly("hu_all", &CDIndexBenchmark::hu_all)
        .def("reset", &CDIndexBenchmark::reset)
        .def("print_summary", &CDIndexBenchmark::print_summary);
    
    // Global benchmark instance
    m.attr("g_benchmark") = py::cast(&g_benchmark, py::return_value_policy::reference);

}