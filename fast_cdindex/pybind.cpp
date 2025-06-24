#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <arrow/python/pyarrow.h>
#include "cdindex_enhanced.h"

namespace py = pybind11;

PYBIND11_MODULE(_cdindex, m) {
    py::module_::import("pyarrow");
    arrow::py::import_pyarrow();

    py::class_<EnhancedGraph>(m, "EnhancedGraph")
        .def(py::init<>())
        .def("add_vertex", &EnhancedGraph::add_vertex)
        .def("add_edge", &EnhancedGraph::add_edge)
        .def("cdindex", &EnhancedGraph::cdindex, py::arg("paper_id"), py::arg("years"))
        .def("cdindex_filtered", &EnhancedGraph::cdindex_filtered,
            py::arg("paper_id"), py::arg("years"), py::arg("filters") = std::unordered_map<std::string, std::vector<int>>())
        .def("cdindex_batch", [](EnhancedGraph &self, py::object arr_obj, int64_t years) {
            PyObject* pobj = arr_obj.ptr();
            auto result_arr = arrow::py::unwrap_array(pobj);
            if (!result_arr.ok()) throw std::runtime_error(result_arr.status().message());
            auto any_arr = *result_arr;
            auto arr = std::static_pointer_cast<arrow::UInt32Array>(any_arr);
            auto table = self.cdindex_batch(arr, years);
            PyObject* out_py = arrow::py::wrap_table(table);
            return py::reinterpret_steal<py::object>(out_py);
        })
        .def("cdindex_filtered_batch", [](EnhancedGraph &self, py::object arr_obj, int64_t years,
                                        const std::unordered_map<std::string, std::vector<int>>& filters) {
            PyObject* pobj = arr_obj.ptr();
            auto result_arr = arrow::py::unwrap_array(pobj);
            if (!result_arr.ok()) throw std::runtime_error(result_arr.status().message());
            auto any_arr = *result_arr;
            auto arr = std::static_pointer_cast<arrow::UInt32Array>(any_arr);
            auto table = self.cdindex_filtered_batch(arr, years, filters);
            PyObject* out_py = arrow::py::wrap_table(table);
            return py::reinterpret_steal<py::object>(out_py);
        })
        .def("ingest_properties", [](EnhancedGraph &self, py::object table_obj) {
            PyObject* pobj = table_obj.ptr();
            auto maybe_table = arrow::py::unwrap_table(pobj);
            if (!maybe_table.ok()) throw std::runtime_error(maybe_table.status().message());
            self.properties.ingest_arrow(*maybe_table);
        })
        .def("build_property_indexes", [](EnhancedGraph &self) {
            self.properties.build_indexes();
        })
        .def("clear_properties", [](EnhancedGraph &self) {
            self.properties.clear();
        })
        .def("clear_filter_cache", &EnhancedGraph::clear_filter_cache)
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
        .def("edge_count", &EnhancedGraph::edge_count);

    // Remove iindex and mcdindex for now since they're not in the enhanced graph
}