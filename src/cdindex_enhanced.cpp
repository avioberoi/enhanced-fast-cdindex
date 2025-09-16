#include "cdindex_enhanced.h"
#include <arrow/api.h>
#include <roaring/roaring.hh>
#include <absl/container/flat_hash_map.h>
#include <omp.h>
#include <mutex>
#include <chrono>
#include <functional>
#include <memory>
#include <cassert>
#include <algorithm>
#include <unordered_set>
#include <cctype>
#include <unordered_set>
#include <absl/strings/string_view.h>
#include <string_view>
#include <array>

// Helper for returning NaN consistently
static inline double return_nan() {
    return std::numeric_limits<double>::quiet_NaN();
}

static int64_t get_env_int(const char* var, int64_t def) {
    if (auto e = std::getenv(var)) try { auto v = std::stoll(e); if (v > 0) return v; } catch(...) {}
    return def;
}
int64_t BATCH_PARALLEL_THRESHOLD() { return get_env_int("BATCH_PARALLEL_THRESHOLD", 10000); }
int64_t INNER_PARALLEL_THRESHOLD() { return get_env_int("INNER_PARALLEL_THRESHOLD", 1000); }
int64_t MAX_CACHE_ENTRIES() { return get_env_int("MAX_CACHE_ENTRIES", 4096); }
int64_t CHUNK_SIZE() { return get_env_int("CHUNK_SIZE", 1000000); }
int64_t get_ingest_chunk_size() { return get_env_int("INGEST_CHUNK_SIZE", 1000000); }

// Global micro-benchmark instance
CDIndexBenchmark g_benchmark;

void CDIndexBenchmark::print_summary() const {
    if (n == 0) {
        std::cerr << "No CD-index computations recorded." << std::endl;
        return;
    }
    
    double total_time = t1 + t2 + t3;
    double avg_time = total_time / n;
    double throughput = n / (total_time / 1000.0);  // comps per second
    
    std::cerr << "\n===== CD-INDEX MICRO-BENCHMARK RESULTS =====\n";
    std::cerr << "Computations: " << n << "\n";
    std::cerr << "Total time: " << total_time << " ms\n";
    std::cerr << "Average time per computation: " << avg_time << " ms\n";
    std::cerr << "Throughput: " << throughput << " comps/sec\n";
    std::cerr << "\nTime breakdown:\n";
    std::cerr << "  T1 (F_t build):     " << t1 << " ms (" << (t1/total_time*100) << "%)\n";
    std::cerr << "  T2 (B_t build):     " << t2 << " ms (" << (t2/total_time*100) << "%)\n";
    std::cerr << "  T3 (Cardinality):   " << t3 << " ms (" << (t3/total_time*100) << "%)\n";
    std::cerr << "\nCache hit rates:\n";
    std::cerr << "  Hf (pred filtered): " << (hf_all > 0 ? (hf_hit * 100.0 / hf_all) : 0.0) 
              << "% (" << hf_hit << "/" << hf_all << ")\n";
    std::cerr << "  Hb (B_any):         " << (hb_all > 0 ? (hb_hit * 100.0 / hb_all) : 0.0) 
              << "% (" << hb_hit << "/" << hb_all << ")\n";
    std::cerr << "  Hu (pred unfilt):   " << (hu_all > 0 ? (hu_hit * 100.0 / hu_all) : 0.0) 
              << "% (" << hu_hit << "/" << hu_all << ")\n";
    std::cerr << "  Hw (window W_t):    " << (hw_all > 0 ? (hw_hit * 100.0 / hw_all) : 0.0) 
              << "% (" << hw_hit << "/" << hw_all << ")\n";
    std::cerr << "============================================\n\n";
}

// Vertex methods
Vertex::Vertex(VertexId id, timestamp_t time) : id(id), time(time) {}
void Vertex::shrink_to_fit() { outgoing_edges.shrink_to_fit(); }
void Vertex::sort_outgoing_edges() {
    std::sort(outgoing_edges.begin(), outgoing_edges.end(), [](const Edge* a, const Edge* b) {
        return a->target->id < b->target->id;
    });
}
bool Vertex::has_outgoing_to(VertexId target_id) const {
    auto it = std::lower_bound(outgoing_edges.begin(), outgoing_edges.end(), target_id,
        [](const Edge* e, const VertexId& id) { return e->target->id < id; });
    return it != outgoing_edges.end() && (*it)->target->id == target_id;
}

// Edge ctor
Edge::Edge(Vertex* s, Vertex* t) : source(s), target(t) {}

// PropertyStore methods…
arrow::Status PropertyStore::ingest_arrow(const std::shared_ptr<arrow::Table>& table) {
    auto id_col_raw = table->GetColumnByName("paper_id");
    if (!id_col_raw) return arrow::Status::Invalid("Table missing 'paper_id' column.");

#ifndef NDEBUG
    std::cerr << "PropertyStore::ingest_arrow: table rows=" << table->num_rows()
              << ", columns=" << table->num_columns()
              << ", chunk_size=" << get_ingest_chunk_size() << std::endl;
    std::cerr << "PropertyStore::ingest_arrow: columns schema: ";
    for (int col_i = 0; col_i < table->num_columns(); ++col_i) {
        auto field = table->schema()->field(col_i);
        std::cerr << field->name() << "(" << field->type()->ToString() << ") ";
    }
    std::cerr << std::endl;
#endif

    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;

    while (true) {
        auto next_batch = reader.Next();
        if (!next_batch.ok()) return next_batch.status();
        batch = *next_batch;
        if (!batch) break;

        // auto ids_array = std::static_pointer_cast<arrow::UInt32Array>(batch->GetColumnByName("paper_id"));

        // Support UINT32 / INT32 / INT64 for paper_id
        auto id_any = batch->GetColumnByName("paper_id");
        if (!id_any) return arrow::Status::Invalid("RecordBatch missing 'paper_id' column.");
        std::shared_ptr<arrow::UInt32Array> ids_u32;
        std::shared_ptr<arrow::Int32Array>  ids_i32;
        std::shared_ptr<arrow::Int64Array>  ids_i64;
        switch (id_any->type()->id()) {
            case arrow::Type::UINT32: ids_u32 = std::static_pointer_cast<arrow::UInt32Array>(id_any); break;
            case arrow::Type::INT32:  ids_i32 = std::static_pointer_cast<arrow::Int32Array>(id_any);  break;
            case arrow::Type::INT64:  ids_i64 = std::static_pointer_cast<arrow::Int64Array>(id_any);  break;
            default:
                return arrow::Status::Invalid("Unsupported 'paper_id' type in PropertyStore::ingest_arrow: ",
                                              id_any->type()->ToString());
        }

        for (int i = 0; i < batch->num_columns(); ++i) {
            const std::string& col_name = batch->schema()->field(i)->name();
            if (col_name == "paper_id") continue;
            auto col = batch->column(i);
            auto type_id = col->type()->id();
            if (type_id == arrow::Type::INT64 || type_id == arrow::Type::INT32) {
                if (type_id == arrow::Type::INT64) {
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(col);
                for (int64_t j = 0; j < col->length(); ++j) {
                    if (!col->IsNull(j)) {
                        // add_categorical(ids_array->Value(j), col_name, static_cast<int>(int_array->Value(j)));
                        VertexId vid = ids_u32 ? static_cast<VertexId>(ids_u32->Value(j))
                                      : ids_i32 ? static_cast<VertexId>(ids_i32->Value(j))
                                                : static_cast<VertexId>(ids_i64->Value(j));
                        add_categorical(vid, col_name, static_cast<int>(int_array->Value(j)));
                    }
                }
                } else {
                    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(col);
                    for (int64_t j = 0; j < col->length(); ++j) {
                        if (!col->IsNull(j)) {
                            // add_categorical(ids_array->Value(j), col_name, static_cast<int>(int32_array->Value(j)));
                            VertexId vid = ids_u32 ? static_cast<VertexId>(ids_u32->Value(j))
                                      : ids_i32 ? static_cast<VertexId>(ids_i32->Value(j))
                                                : static_cast<VertexId>(ids_i64->Value(j));
                            add_categorical(vid, col_name, static_cast<int>(int32_array->Value(j)));
                        }
                    }
                }
            } else if ((type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING)
                        && col_name == "country") {
                // Cast to concrete string array types and use GetString(i)
                std::shared_ptr<arrow::StringArray> s_arr;
                std::shared_ptr<arrow::LargeStringArray> ls_arr;
                if (type_id == arrow::Type::STRING) s_arr  = std::static_pointer_cast<arrow::StringArray>(col);
                else ls_arr = std::static_pointer_cast<arrow::LargeStringArray>(col);
                auto& dict = string_dictionaries_[col_name];
                // Pre-reserve dictionary capacity to avoid rehashing
                if (dict.empty()) {
                    dict.reserve(100);  // Estimate based on typical categorical cardinality
                }
                for (int64_t j = 0; j < col->length(); ++j) {
                    if (!col->IsNull(j)) {
                        VertexId vid = ids_u32 ? static_cast<VertexId>(ids_u32->Value(j))
                                      : ids_i32 ? static_cast<VertexId>(ids_i32->Value(j))
                                                : static_cast<VertexId>(ids_i64->Value(j));
                        std::string val = (s_arr ? s_arr->GetString(j) : ls_arr->GetString(j));
                        // normalize lowercase (countries already normalized, but safe)
                        for (auto& ch : val) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        int code;
                        auto it = dict.find(val);
                        if (it == dict.end()) {
                            code = dict.size() + 1;
                            dict[val] = code;
                        } else {
                            code = it->second;
                        }
                        add_categorical(vid, col_name, code);
                    }
                }
            } else if (type_id == arrow::Type::BOOL) {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(col);
                for (int64_t j = 0; j < col->length(); ++j) {
                    if (!col->IsNull(j)) {
                        int code = bool_array->Value(j) ? 1 : 0;
                        // add_categorical(ids_array->Value(j), col_name, code);
                        VertexId vid = ids_u32 ? static_cast<VertexId>(ids_u32->Value(j))
                                      : ids_i32 ? static_cast<VertexId>(ids_i32->Value(j))
                                                : static_cast<VertexId>(ids_i64->Value(j));
                        add_categorical(vid, col_name, code);
                    }
                }
            } else {
                std::cerr << "PropertyStore::ingest_arrow: warning, skipping unsupported column '"
                          << col_name << "' of type " << col->type()->ToString() << std::endl;
            }
        }
    }

    size_t total_entries = 0;
    for (auto const& kv : categorical_properties_) {
        total_entries += kv.second.size();
    }
#ifndef NDEBUG
    std::cerr << "PropertyStore::ingest_arrow: completed, loaded " << total_entries
              << " property entries across " << categorical_properties_.size()
              << " columns." << std::endl;
#endif
    return arrow::Status::OK();
}
void PropertyStore::build_indexes() {
    // Clear any old data
    categorical_bitmaps_.clear();

    // Pre-allocate one map entry per property (so no concurrent inserts later)
    // categorical_bitmaps_.reserve(categorical_properties_.size());
    // for (auto const& kv : categorical_properties_) {
    //     categorical_bitmaps_.emplace(
    //         kv.first, // property name
    //         absl::flat_hash_map<int, Roaring>()
    //     );
    // }

    // We only need bitmaps for "year" (and "country" if used for regions).
    categorical_bitmaps_.reserve(2);
    auto itY = categorical_properties_.find("year");
    if (itY != categorical_properties_.end())
        categorical_bitmaps_.emplace("year", absl::flat_hash_map<int, Roaring>());
    auto itC = categorical_properties_.find("country");
    if (itC != categorical_properties_.end())
        categorical_bitmaps_.emplace("country", absl::flat_hash_map<int, Roaring>());

    // Gather property names into a vector for simple indexing
    std::vector<std::string> prop_names;
    // prop_names.reserve(categorical_properties_.size());
    // for (auto const& kv : categorical_properties_) {
    //     prop_names.push_back(kv.first);
    // }

    if (itY != categorical_properties_.end()) prop_names.push_back("year");
    if (itC != categorical_properties_.end()) prop_names.push_back("country");

    // Parallel loop over properties
    #pragma omp parallel for schedule(dynamic)
    for (size_t pi = 0; pi < prop_names.size(); ++pi) {
        const auto& prop = prop_names[pi];
        auto const& vec  = categorical_properties_.at(prop);
        auto&       bmp_map = categorical_bitmaps_.at(prop);

        // Estimate distinct values by scanning the property vector
        std::unordered_set<int> distinct_values;
        for (auto const& [id, val] : vec) {
            distinct_values.insert(val);
        }
        
        // Reserve based on actual distinct value count
        bmp_map.reserve(distinct_values.size());
        
        // Build bitmaps for each value
        for (auto const& [id, val] : vec) {
            bmp_map[val].add(id);
        }
    }
    
    // DISABLED: build_prefix_or_arrays() - was causing memory explosion (119GB RAM)
    // Year bitmaps already built in categorical_bitmaps_["year"] during the loop above
}
void PropertyStore::add_categorical(VertexId id, const std::string& name, int value) { 
    // Pre-reserve container to avoid frequent reallocations
    auto& vec = categorical_properties_[name];
    if (vec.empty()) {
        // Estimate initial capacity - adjust based on typical dataset characteristics
        vec.reserve(1000);  
    }
    vec.emplace_back(id, value);
}
Roaring PropertyStore::get_combined_bitmap(const std::string& prop_name, const std::vector<int>& values) const {
    Roaring result;
    auto it = categorical_bitmaps_.find(prop_name);
    if (it != categorical_bitmaps_.end()) {
        for (int val : values) {
            auto val_it = it->second.find(val);
            if (val_it != it->second.end()) {
                result |= val_it->second;
            }
        }
    }
    return result;
}

std::vector<int> PropertyStore::get_codes_for_strings(const std::string& prop,
                                                        const std::vector<std::string>& names) const {
    std::vector<int> out;
    auto it = string_dictionaries_.find(prop);
    if (it == string_dictionaries_.end()) return out;
    out.reserve(names.size());
    for (auto s : names) {
        for (auto& ch : s) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        auto jt = it->second.find(s);
        if (jt != it->second.end()) out.push_back(jt->second);
    }
    return out;
}

Roaring PropertyStore::get_combined_bitmap_str(const std::string& prop_name,
                                                const std::vector<std::string>& names) const {
    return get_combined_bitmap(prop_name, get_codes_for_strings(prop_name, names));
}


void PropertyStore::clear() { 
    categorical_properties_.clear(); 
    categorical_bitmaps_.clear(); 
    // REMOVED: prefix_or_bitmaps_.clear(); - removed the entire data structure
    string_dictionaries_.clear();
}

// Build W_t by unioning year bitmaps (no prefix storage needed)
Roaring PropertyStore::get_window_bitmap_by_union(const std::string& prop_name, int start_year, int end_year) const {
    // Handle degenerate windows early
    if (end_year <= start_year) {
        return Roaring();  // Empty window
    }
    
    // Get the year bitmap map
    auto it = categorical_bitmaps_.find(prop_name);
    if (it == categorical_bitmaps_.end()) {
        return Roaring();  // Property not found
    }
    const auto& year_map = it->second;

    // Collect pointers to the years we have (sparse years OK)
    // Window is (start_year, end_year], so loop from start_year+1 to end_year
    std::vector<const Roaring*> parts;
    parts.reserve(std::max(0, end_year - start_year));
    
    for (int y = start_year + 1; y <= end_year; ++y) {
        auto yit = year_map.find(y);
        if (yit != year_map.end()) {
            parts.push_back(&yit->second);
        }
    }
    
    // Handle empty parts case
    if (parts.empty()) {
        return Roaring();  // No years found in window
    }
    
    // Union the bitmaps
    Roaring W;
    if (parts.size() == 1) {
        W = *parts[0];
    } else if (parts.size() > 1) {
        static const bool kNoFastUnion = (std::getenv("CDINDEX_NO_FASTUNION") != nullptr);
        if (!kNoFastUnion) {
            // If your CRoaring is old/new-mismatched, this is where a segfault can happen.
            // The switch lets you avoid it without rebuilding.
            W = Roaring::fastunion(parts.size(), parts.data());
        } else {
            for (const auto* p : parts) W |= *p;
        }
    }
    
    // Optimize only for larger unions (small ones don't benefit much)
    if (parts.size() > 2) {
        W.runOptimize();
    }
    
    return W;
}

arrow::Status PropertyStore::ingest_country_direct(const std::shared_ptr<arrow::Table>& table,
                                                   const std::unordered_map<std::string, VertexId>& uid2id,
                                                   const std::string& uid_col,
                                                   const std::string& country_col) {
    if (!table) return arrow::Status::Invalid("null table for ingest_country_direct");
    auto uid_arr = table->GetColumnByName(uid_col);
    auto cn_arr  = table->GetColumnByName(country_col);
    if (!uid_arr || !cn_arr) {
        return arrow::Status::Invalid("country table missing UID or country column");
    }
    // Ensure bitmap map exists
    auto& bmp_map = categorical_bitmaps_["country"]; // value->bitmap
    auto& dict    = string_dictionaries_["country"]; // string->code
    // Iterate in batches
    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        ARROW_ASSIGN_OR_RAISE(batch, reader.Next());
        if (!batch) break;
        auto uid_col_arr = batch->GetColumnByName(uid_col);
        auto c_col_arr   = batch->GetColumnByName(country_col);
        if (!uid_col_arr || !c_col_arr) continue;
        
        const auto uid_tid = uid_col_arr->type()->id();
        const auto c_tid   = c_col_arr->type()->id();
        std::shared_ptr<arrow::StringArray> u_str;
        std::shared_ptr<arrow::LargeStringArray> u_lstr;
        std::shared_ptr<arrow::UInt32Array> u_u32;
        std::shared_ptr<arrow::Int32Array> u_i32;
        std::shared_ptr<arrow::Int64Array> u_i64;
        std::shared_ptr<arrow::StringArray> c_str;
        std::shared_ptr<arrow::LargeStringArray> c_lstr;
        if (uid_tid == arrow::Type::STRING) u_str  = std::static_pointer_cast<arrow::StringArray>(uid_col_arr);
        else if (uid_tid == arrow::Type::LARGE_STRING) u_lstr = std::static_pointer_cast<arrow::LargeStringArray>(uid_col_arr);
        else if (uid_tid == arrow::Type::UINT32) u_u32  = std::static_pointer_cast<arrow::UInt32Array>(uid_col_arr);
        else if (uid_tid == arrow::Type::INT32) u_i32  = std::static_pointer_cast<arrow::Int32Array>(uid_col_arr);
        else if (uid_tid == arrow::Type::INT64) u_i64  = std::static_pointer_cast<arrow::Int64Array>(uid_col_arr);
        if (c_tid == arrow::Type::STRING) c_str  = std::static_pointer_cast<arrow::StringArray>(c_col_arr);
        else if (c_tid == arrow::Type::LARGE_STRING) c_lstr = std::static_pointer_cast<arrow::LargeStringArray>(c_col_arr);
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            // Skip nulls
            if (uid_col_arr->IsNull(i) || c_col_arr->IsNull(i)) continue;
            std::string uid;
            if (u_str)      uid = u_str->GetString(i);
            else if (u_lstr)uid = u_lstr->GetString(i);
            else if (u_u32) uid = std::to_string(u_u32->Value(i));
            else if (u_i32) uid = std::to_string(u_i32->Value(i));
            else if (u_i64) uid = std::to_string(u_i64->Value(i));
            else continue; // unsupported UID type
            auto it_id = uid2id.find(uid);
            if (it_id == uid2id.end()) continue; // UID not present in graph
            std::string country = c_str ? c_str->GetString(i) : (c_lstr ? c_lstr->GetString(i) : std::string());
            if (country.empty()) continue;
            // normalized already, but enforce lowercase just in case
            for (auto& ch : country) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            // intern country string to code
            int code;
            auto it = dict.find(country);
            if (it == dict.end()) {
                code = static_cast<int>(dict.size()) + 1;
                dict[country] = code;
            } else {
                code = it->second;
            }
            bmp_map[code].add(it_id->second);
        }
    }
    return arrow::Status::OK();
}
static Roaring build_ref_bitmap(const std::vector<Vertex*>& refs, timestamp_t f, time_delta_t d) {
    Roaring r;
    for (auto* v : refs) if (v->time > f && v->time <= f + d) r.add(v->id);
    return r;
}

// Optimized binary search version (requires sorted refs by time)
static Roaring build_ref_bitmap_sorted(const std::vector<Vertex*>& refs, timestamp_t f, time_delta_t d) {
    if (refs.empty()) return Roaring();
    
    // Binary search for papers with time > f
    auto lb = std::upper_bound(refs.begin(), refs.end(), f,
        [](timestamp_t t, const Vertex* v) { return t < v->time; });
    
    // Binary search for papers with time <= f + d  
    auto ub = std::upper_bound(refs.begin(), refs.end(), f + d,
        [](timestamp_t t, const Vertex* v) { return t < v->time; });
    
    Roaring r;
    for (auto it = lb; it != ub; ++it) {
        r.add((*it)->id);
    }
    return r;
}

// Graph methods…
Graph::~Graph() {
    for (auto& kv : vertices_) {
        Vertex* v = kv.second;
        for (Edge* e : v->outgoing_edges) delete e;
        delete v;
    }
}
void Graph::add_vertex(VertexId id, timestamp_t time) {
    if (vertices_.find(id) == vertices_.end()) {
        vertices_[id] = new Vertex(id, time);
    }
}
void Graph::add_edge(VertexId source_id, VertexId target_id) {
    auto source_it = vertices_.find(source_id);
    auto target_it = vertices_.find(target_id);
    if (source_it != vertices_.end() && target_it != vertices_.end()) {
        Vertex* source = source_it->second;
        Vertex* target = target_it->second;
        Edge* new_edge = new Edge(source, target);
        source->outgoing_edges.push_back(new_edge);
        incoming_edges_[target_id].push_back(source);
    }
}
double Graph::cdindex(VertexId focal_id, time_delta_t dt) { 
    // Delegate to unified core with no region filter
    return cdindex_core(focal_id, dt, nullptr, true, nullptr, nullptr, nullptr);
}
#ifndef NDEBUG
#define CDTRACE(stage, val) do { \
    std::cerr << "[cd] " << stage << ": " << (val) << std::endl; \
} while(0)
#else
#define CDTRACE(stage, val) do { } while(0)
#endif

// SAFE AND_CARDINALITY: Avoid fragile SIMD paths in certain CRoaring builds
static inline uint64_t and_cardinality_safe(const Roaring& a, const Roaring& b) {
    // Default to fast native method; set CDINDEX_ANDCARD_SAFE=1 to use slower fallback
    static const bool use_safe = (std::getenv("CDINDEX_ANDCARD_SAFE") != nullptr);

    if (use_safe) {
        Roaring tmp = a & b;     // robust path (slower but safe)
        return tmp.cardinality();
    } else {
        return a.and_cardinality(b);  // fast native path (default)
    }
}

// DEBUG: Validate roaring bitmap integrity
#ifndef NDEBUG
static inline void check_roaring_valid(const char* name, const Roaring& r) {
    try {
        // Basic sanity check - try a simple operation
        uint64_t card = r.cardinality();
        (void)card; // Suppress unused variable warning
        
        // Test copy operation to catch corruption early
        Roaring test_copy = r;
        if (test_copy.cardinality() != card) {
            std::cerr << "[fatal] roaring bitmap copy cardinality mismatch: " << name << std::endl;
            std::abort();
        }
    } catch (const std::exception& e) {
        std::cerr << "[fatal] invalid roaring bitmap '" << name << "': " << e.what() << std::endl;
        std::abort();
    } catch (...) {
        std::cerr << "[fatal] invalid roaring bitmap '" << name << "': unknown error" << std::endl;
        std::abort();
    }
}
#else
static inline void check_roaring_valid(const char*, const Roaring&) { /* no-op in release */ }
#endif

double Graph::cdindex_core(VertexId fid, time_delta_t dt, const Roaring* region, bool include_region, 
                            const Roaring* F_t_cached, const Roaring* B_any_cached, const Roaring* W_t_cached) {
    CDTRACE("ENTER", fid);
    auto fit = vertices_.find(fid);
    if (fit == vertices_.end()) return return_nan();
    const Vertex* focal = fit->second;
    const timestamp_t ft = focal->time;
    
    // Step 1: Build or reuse F_t (time-filtered citers of focal)
    const Roaring* Fp = F_t_cached;
    Roaring F_local;
    if (!Fp) {
        ScopedTimer _t1(g_benchmark.t1);
        auto focal_citers_it = incoming_edges_.find(fid);
        if (focal_citers_it != incoming_edges_.end()) {
            F_local = incoming_edges_sorted_by_time_
                        ? build_ref_bitmap_sorted(focal_citers_it->second, ft, dt)
                        : build_ref_bitmap        (focal_citers_it->second, ft, dt);
        }
        Fp = &F_local;
    }
    CDTRACE("F_t.size", Fp->cardinality());
    
    // Step 2: Get B_any and W_t with safe shared ownership (or reuse cached)
    std::shared_ptr<const Roaring> B_any_sp, W_t_sp;
    const Roaring* B_any_ptr = nullptr;
    const Roaring* W_t_ptr = nullptr;
    
    {
        ScopedTimer _t2(g_benchmark.t2);
        if (B_any_cached && W_t_cached) {
            // FAST PATH: Reuse caller-provided cached pointers (cdindex_all uses this)
            B_any_ptr = B_any_cached;
            W_t_ptr   = W_t_cached;
        } else {
            // SLOW PATH: Need to build/cache B_any and W_t (single cdindex calls use this)
            const auto* enhanced_graph = dynamic_cast<const EnhancedGraph*>(this);
            if (enhanced_graph) {
                // Enhanced path: get cached B_any and W_t with shared ownership
                CDTRACE("B_any.pre", 0);
                B_any_sp = enhanced_graph->get_bany_for_focal_sp(fid);
                B_any_ptr = B_any_sp.get();
                CDTRACE("B_any.size", B_any_ptr ? B_any_ptr->cardinality() : 0);
                
                CDTRACE("W_t.pre", 0);
                W_t_sp = enhanced_graph->get_window_bitmap_sp(ft, dt);
                W_t_ptr = W_t_sp.get();
                CDTRACE("W_t.size", W_t_ptr ? W_t_ptr->cardinality() : 0);
            } else {
                // Fallback for base Graph class: build B_any and W_t locally
                auto B_any_local = std::make_shared<Roaring>();
                for (const auto* e : focal->outgoing_edges) {
                    VertexId b = e->target->id;
                    auto it = incoming_edges_.find(b);
                    if (it != incoming_edges_.end()) {
                        for (const auto* c : it->second) B_any_local->add(c->id);
                    }
                }
                B_any_sp = B_any_local;
                B_any_ptr = B_any_sp.get();
                
                auto W_t_local = std::make_shared<Roaring>();
                for (const auto& [vid, vertex] : vertices_) {
                    if (vertex->time > ft && vertex->time <= ft + dt) {
                        W_t_local->add(vid);
                    }
                }
                W_t_sp = W_t_local;
                W_t_ptr = W_t_sp.get();
            }
        }
    }
    
    // Step 3: Compute cardinalities with optional region filter
    ScopedTimer _t3(g_benchmark.t3);
    
    // FAST PATH 1: Empty F_t with no filter
    const uint64_t cF = Fp->cardinality();
    if (!region && cF == 0) {
        // Only need bwin_all for denominator
        const uint64_t bwin_all = and_cardinality_safe(*B_any_ptr, *W_t_ptr);
        if (bwin_all == 0) {
            return return_nan();  // Empty i-set
        }
        ++g_benchmark.n;  // Count successful computation
        // Numerator = 0, Denominator = bwin_all
        return 0.0;
    }
    
    // Handle region filtering efficiently
    uint64_t cF_C, cFB_C, bwin_C;
    
    if (!region) {
        // No filter: compute unfiltered totals only here
        CDTRACE("and1.pre", 0);
        const uint64_t cFB_all = and_cardinality_safe(*Fp, *B_any_ptr);
        CDTRACE("and1.post", cFB_all);
        CDTRACE("and2.pre", 0);
        const uint64_t bwin_all = and_cardinality_safe(*B_any_ptr, *W_t_ptr);
        CDTRACE("and2.post", bwin_all);
        cF_C = cF;
        cFB_C = cFB_all;
        bwin_C = bwin_all;
    } else {
        // Region present: Early short-circuit for empty F_t cases
        if (cF == 0) {
            // With empty F_t, numerator is always 0; only denominator matters
            CDTRACE("region.empty_F", 1);
            
            // DEBUG: Validate bitmap integrity before critical operations
            check_roaring_valid("B_any", *B_any_ptr);
            check_roaring_valid("W_t", *W_t_ptr);
            check_roaring_valid("region", *region);
            
            Roaring W_R = *W_t_ptr & *region; // Small: W_t ∩ R
            check_roaring_valid("W_R", W_R);
            
            const uint64_t bwin_R = and_cardinality_safe(*B_any_ptr, W_R);
            ++g_benchmark.n;
            if (bwin_R == 0) return return_nan();
            return 0.0;
        }
        
        // Region present: Build small intermediates for non-empty F_t
        Roaring F_R = (*Fp) & *region;    // Small: F_t ∩ R
        Roaring W_R = *W_t_ptr & *region; // Small: W_t ∩ R  
        
        // FAST PATH 2: Empty region overlap
        if (F_R.isEmpty() && W_R.isEmpty() && include_region) {
            return return_nan();  // Empty filtered i-set
        }
        
        const uint64_t cF_R = F_R.cardinality();    // |F_t ∩ R|
        const uint64_t cFB_R  = (cF_R ? and_cardinality_safe(*B_any_ptr, F_R) : 0);   // |F_t ∩ B_any ∩ R|
        const uint64_t bwin_R = and_cardinality_safe(*B_any_ptr, W_R);   // |B_any ∩ W_t ∩ R|
        
        if (include_region) {
            // Include only papers in region (OnlyUS, OnlyCN, OnlyEU)
            cF_C = cF_R;
            cFB_C = cFB_R;
            bwin_C = bwin_R;
        } else {
            // Exclude papers in region (ExcludeUS, ExcludeCN, ExcludeEU)
            // Use complement in counts, not complement bitmaps
            const uint64_t cFB_all  = and_cardinality_safe(*Fp, *B_any_ptr);
            const uint64_t bwin_all = and_cardinality_safe(*B_any_ptr, *W_t_ptr);
            cF_C   = cF - cF_R;
            cFB_C  = cFB_all - cFB_R;
            bwin_C = bwin_all - bwin_R;
            #ifndef NDEBUG
                assert(cFB_all <= cF);
            #endif
        }
        
        // Debug assertions (only in debug builds)
        #ifndef NDEBUG
            assert(cFB_C <= cF_C);       // |F∩B| ≤ |F|
        #endif
    }
    
    // Final CD-index calculation
    // Denominator: |F_t ∪ B_t| = |F_t| + |B_any ∩ W_t| - |F_t ∩ B_any|
    const uint64_t denom = cF_C + bwin_C - cFB_C;
    if (denom == 0) return return_nan();

    #ifndef NDEBUG
    // Optional: fast union parity check in debug builds
    Roaring U = (*Fp);
    if (!region) U |= (*B_any_ptr & *W_t_ptr);
    // If region present, parity check is less meaningful without reconstructing filtered union.
    #endif

    
    ++g_benchmark.n;  // Count successful computation
    
    // Numerator: |F_t| - 2*|F_t ∩ B_any|
    const double numerator = static_cast<double>(cF_C) - 2.0 * static_cast<double>(cFB_C);
    return numerator / static_cast<double>(denom);
}

double Graph::cdindex_filtered(VertexId fid, time_delta_t dt, CiterFilter filt) {
    // Delegate to unified core with appropriate region filter
    const auto* eg = dynamic_cast<const EnhancedGraph*>(this);
    const Roaring* region = eg ? eg->region_bitmap_for(filt) : nullptr;
    
    const bool include_region = (filt == CiterFilter::OnlyUS || 
                                 filt == CiterFilter::OnlyCN || 
                                 filt == CiterFilter::OnlyEU);
    
    return cdindex_core(fid, dt, region, include_region, nullptr, nullptr, nullptr);
}

// Build F_t once and call cdindex_core 7× with the cached F_t
std::array<double,7> EnhancedGraph::cdindex_all(VertexId fid, time_delta_t dt) const {
    // Allow access to members from a const method with no mutation of graph state
    auto* self = const_cast<EnhancedGraph*>(this);
    auto fit = self->vertices_.find(fid);
    if (fit == self->vertices_.end()) {
        return {return_nan(),return_nan(),return_nan(),return_nan(),return_nan(),return_nan(),return_nan()};
    }
    const timestamp_t ft = fit->second->time;
    
    Roaring F_t;
    {
        ScopedTimer _t1(g_benchmark.t1);
        auto focal_citers_it = self->incoming_edges_.find(fid);
        if (focal_citers_it != self->incoming_edges_.end()) {
            F_t = self->incoming_edges_sorted_by_time_
                    ? build_ref_bitmap_sorted(focal_citers_it->second, ft, dt)
                    : build_ref_bitmap        (focal_citers_it->second, ft, dt);
        }
    }

    // Fetch B_any & W_t ONCE (hold shared_ptrs locally to keep them alive)
    auto B_any_sp = self->get_bany_for_focal_sp(fid);
    auto W_t_sp   = self->get_window_bitmap_sp(ft, dt);
    const Roaring* Bp = B_any_sp.get();
    const Roaring* Wp = W_t_sp.get();

    // Region bitmaps (no rebuild here; uses whatever is already set up)
    const Roaring* US = self->region_bitmap_for(CiterFilter::OnlyUS);
    const Roaring* EU = self->region_bitmap_for(CiterFilter::OnlyEU);
    const Roaring* CN = self->region_bitmap_for(CiterFilter::OnlyCN);
    
    // Call cdindex_core with cached F_t for all 7 variants
    double cd_base    = self->cdindex_core(fid, dt, nullptr, false, &F_t, Bp, Wp);
    double cd_only_us = self->cdindex_core(fid, dt, US, true,  &F_t, Bp, Wp);
    double cd_excl_us = self->cdindex_core(fid, dt, US, false, &F_t, Bp, Wp);
    double cd_only_eu = self->cdindex_core(fid, dt, EU, true,  &F_t, Bp, Wp);
    double cd_excl_eu = self->cdindex_core(fid, dt, EU, false, &F_t, Bp, Wp);
    double cd_only_cn = self->cdindex_core(fid, dt, CN, true,  &F_t, Bp, Wp);
    double cd_excl_cn = self->cdindex_core(fid, dt, CN, false, &F_t, Bp, Wp);
    
    return {cd_base, cd_only_us, cd_excl_us, cd_only_eu, cd_excl_eu, cd_only_cn, cd_excl_cn};
}

// SAFE REGION SERIALIZATION: Portable format with enhanced validation
static bool write_roar_safe(const Roaring& r, const std::string& path) {
    FILE* f = fopen(path.c_str(), "wb"); 
    if (!f) return false;
    
    // Use portable format for cross-platform/cross-build compatibility
    const size_t sz = r.getSizeInBytes(true);  // portable=true
    std::vector<char> buf(sz);
    r.write(buf.data(), true);  // portable=true
    const size_t w = fwrite(buf.data(), 1, sz, f);
    fclose(f);
    
    std::cerr << "[roaring] Wrote " << sz << " bytes (portable) to " << path << " (cardinality=" << r.cardinality() << ")" << std::endl;
    return w == sz;
}

static bool read_roar_safe(Roaring& r, const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb"); 
    if (!f) return false;
    
    fseek(f, 0, SEEK_END); 
    long sz = ftell(f); 
    fseek(f, 0, SEEK_SET);
    if (sz <= 0) { fclose(f); return false; }
    
    std::vector<char> buf(sz);
    if (fread(buf.data(), 1, sz, f) != (size_t)sz) { 
        fclose(f); 
        return false; 
    }
    fclose(f);
    
    try {
        // Try portable format first, then fall back to native
        r = Roaring::read(buf.data(), true);  // portable=true
        
        // Basic sanity check - try a simple operation
        uint64_t card = r.cardinality();
        std::cerr << "[roaring] Read " << sz << " bytes (portable) from " << path << " (cardinality=" << card << ")" << std::endl;
        
        // Test basic operation to catch corruption early
        Roaring test_copy = r;
        if (test_copy.cardinality() != card) {
            std::cerr << "[roaring] Copy cardinality mismatch for " << path << std::endl;
            return false;
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "[roaring] Exception reading portable format from " << path << ": " << e.what() << std::endl;
        
        // Fallback to native format
        try {
            r = Roaring::read(buf.data(), false);  // portable=false
            uint64_t card = r.cardinality();
            std::cerr << "[roaring] Fallback: Read " << sz << " bytes (native) from " << path << " (cardinality=" << card << ")" << std::endl;
            return true;
        } catch (const std::exception& e2) {
            std::cerr << "[roaring] Exception reading native format from " << path << ": " << e2.what() << std::endl;
            return false;
        }
    }
}

// DEBUG: Verify incoming edges pointer integrity
bool EnhancedGraph::verify_incoming_integrity(size_t samples) const {
    size_t checked = 0;
    for (const auto& kv : incoming_edges_) {
        VertexId t = kv.first;
        for (const Vertex* p : kv.second) {
            if (!p) { 
                std::cerr << "[verify] null ptr at target " << t << std::endl; 
                return false; 
            }
            if (vertices_.find(p->id) == vertices_.end()) {
                std::cerr << "[verify] dangling ptr id=" << p->id << " at target " << t << std::endl;
                return false;
            }
            if (++checked >= samples) return true;
        }
    }
    return true;
}

// DEBUG: Verify region bitmaps work correctly with time windows
bool EnhancedGraph::verify_regions_against_years(timestamp_t ft, time_delta_t dt) const {
    try {
        auto W = get_window_bitmap_sp(ft, dt);
        if (!W) return false;
        
        // Test intersections with each region
        auto tmp_us = *W & regions_.us; 
        (void)tmp_us.cardinality();
        
        auto tmp_eu = *W & regions_.eu; 
        (void)tmp_eu.cardinality();
        
        auto tmp_cn = *W & regions_.cn; 
        (void)tmp_cn.cardinality();
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "[verify_regions] Exception: " << e.what() << std::endl;
        return false;
    } catch (...) {
        std::cerr << "[verify_regions] Unknown exception" << std::endl;
        return false;
    }
}
  
size_t Graph::iindex(VertexId focal_id, time_delta_t dt) {
    auto fit = vertices_.find(focal_id);
    if (fit == vertices_.end()) return 0;

    const timestamp_t ft = fit->second->time;
    const auto it = incoming_edges_.find(focal_id);
    if (it == incoming_edges_.end() || it->second.empty()) return 0;

    // Legacy behavior: no lower bound; count citer->time <= ft+dt
    if (incoming_edges_sorted_by_time_) {
        const auto& v = it->second;
        const auto ub = std::upper_bound(
            v.begin(), v.end(), ft + dt,
            [](timestamp_t t, const Vertex* x) { return t < x->time; });
        return static_cast<size_t>(ub - v.begin());
    }

    // Fallback: linear scan
    size_t count = 0;
    for (const Vertex* citer : it->second) if (citer->time <= ft + dt) ++count;
    return count;
}

double Graph::mcdindex(VertexId focal_id, time_delta_t dt) {
    const double cd = cdindex(focal_id, dt);
    const size_t ii = iindex(focal_id, dt);
    if (ii == 0) return return_nan();
    return cd * static_cast<double>(ii);
}

size_t Graph::in_degree(VertexId id) const {
    auto it = incoming_edges_.find(id);
    return it != incoming_edges_.end() ? it->second.size() : 0;
}

size_t Graph::out_degree(VertexId id) const {
    auto it = vertices_.find(id);
    return it != vertices_.end() ? it->second->outgoing_edges.size() : 0;
}

std::vector<VertexId> Graph::in_edges(VertexId id) const {
    std::vector<VertexId> result;
    auto it = incoming_edges_.find(id);
    if (it != incoming_edges_.end()) {
        result.reserve(it->second.size());
        for (auto* v : it->second) {
            result.push_back(v->id);
        }
    }
    return result;
}

std::vector<VertexId> Graph::out_edges(VertexId id) const {
    std::vector<VertexId> result;
    auto it = vertices_.find(id);
    if (it != vertices_.end()) {
        const auto& edges = it->second->outgoing_edges;
        result.reserve(edges.size());
        for (auto* edge : edges) {
            result.push_back(edge->target->id);
        }
    }
    return result;
}

timestamp_t Graph::get_timestamp(VertexId id) const {
    auto it = vertices_.find(id);
    return it != vertices_.end() ? it->second->time : 0;
}

void Graph::prepare_for_searching() {
    for (auto& kv : vertices_) {
        kv.second->shrink_to_fit();
        kv.second->sort_outgoing_edges();
    }
    // Also sort incoming edges by time for binary search optimization
    sort_incoming_edges_by_time();
}

void Graph::sort_incoming_edges_by_time() {
    if (incoming_edges_sorted_by_time_) return;  // Already sorted
    
    for (auto& kv : incoming_edges_) {
        std::sort(kv.second.begin(), kv.second.end(), 
                  [](const Vertex* a, const Vertex* b) {
                      return a->time < b->time;
                  });
    }
    incoming_edges_sorted_by_time_ = true;
}
std::vector<VertexId> Graph::get_citers(VertexId focal_id, time_delta_t t_delta) {
    // Return all papers that cite the focal paper within the time window
    auto vit = vertices_.find(focal_id);
    if (vit == vertices_.end()) return {};
    Vertex* focal_paper = vit->second;
    timestamp_t focal_time = focal_paper->time;
    std::vector<VertexId> citers;
    // Incoming edges map holds citers of the focal (source vertices)
    auto in_it = incoming_edges_.find(focal_id);
    if (in_it == incoming_edges_.end()) return citers;
    for (Vertex* citer : in_it->second) {
        if (citer->time > focal_time && citer->time <= focal_time + t_delta) {
            citers.push_back(citer->id);
        }
    }
    return citers;
}

// EnhancedGraph methods…
void EnhancedGraph::add_vertices_from_arrow(const std::shared_ptr<arrow::Table>& table) {
    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;
    const bool have_uid = (table->GetColumnByName("UID") != nullptr);
    if (have_uid) uid2id_.reserve(static_cast<size_t>(table->num_rows()));
    while (true) {
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> next_batch = reader.Next();
        if (!next_batch.ok()) {
            throw std::runtime_error(next_batch.status().message());
        }
        batch = *next_batch;
        if (!batch) break;
        // Optional UID column (string) to build internal UID→id map
        std::shared_ptr<arrow::Array> uid_col = batch->GetColumnByName("UID");
        std::shared_ptr<arrow::StringArray> uid_str;
        std::shared_ptr<arrow::LargeStringArray> uid_lstr;
        if (uid_col) {
            if (uid_col->type()->id() == arrow::Type::STRING){
                uid_str = std::static_pointer_cast<arrow::StringArray>(uid_col);
                uid_owner_chunks_.push_back(uid_str);
            }
            else if (uid_col->type()->id() == arrow::Type::LARGE_STRING){
                uid_lstr = std::static_pointer_cast<arrow::LargeStringArray>(uid_col);
                uid_owner_chunks_.push_back(uid_lstr);
            }
        }
        // Handle flexible ID types: UINT32, INT64, INT32
        auto id_col = batch->GetColumnByName("paper_id");
        std::shared_ptr<arrow::UInt32Array> ids32;
        std::shared_ptr<arrow::Int64Array> ids64;
        std::shared_ptr<arrow::Int32Array> ids_i32;
        switch (id_col->type()->id()) {
            case arrow::Type::UINT32:
                ids32   = std::static_pointer_cast<arrow::UInt32Array>(id_col);
                break;
            case arrow::Type::INT64:
                ids64   = std::static_pointer_cast<arrow::Int64Array>(id_col);
                break;
            case arrow::Type::INT32:
                ids_i32 = std::static_pointer_cast<arrow::Int32Array>(id_col);
                break;
            default:
                throw std::runtime_error("Unsupported paper_id column type " + id_col->type()->ToString());
        }
        // Handle flexible year types: INT64, INT32
        auto year_col = batch->GetColumnByName("year");
        std::shared_ptr<arrow::Int64Array> years64;
        std::shared_ptr<arrow::Int32Array> years32;
        switch (year_col->type()->id()) {
            case arrow::Type::INT64:
                years64 = std::static_pointer_cast<arrow::Int64Array>(year_col);
                break;
            case arrow::Type::INT32:
                years32 = std::static_pointer_cast<arrow::Int32Array>(year_col);
                break;
            default:
                throw std::runtime_error("Unsupported year column type " + year_col->type()->ToString());
        }
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            // Extract year value
            timestamp_t t;
            if (years64) {
                if (years64->IsNull(i)) continue;
                t = years64->Value(i);
            } else {
                if (years32->IsNull(i)) continue;
                t = static_cast<timestamp_t>(years32->Value(i));
            }
            // Extract ID
            VertexId vid;
            if (ids64) {
                if (ids64->IsNull(i)) continue;
                vid = static_cast<VertexId>(ids64->Value(i));
            } else if (ids_i32) {
                if (ids_i32->IsNull(i)) continue;
                vid = static_cast<VertexId>(ids_i32->Value(i));
            } else {
                if (ids32->IsNull(i)) continue;
                vid = ids32->Value(i);
            }
            add_vertex(vid, t);
            // If UID present, remember mapping (absl::string_view into Arrow buffer)
            if (uid_str && !uid_str->IsNull(i)) {
                const int32_t off = uid_str->value_offset(i);
                const int32_t len = uid_str->value_length(i);
                const char*   ptr = reinterpret_cast<const char*>(uid_str->value_data()->data()) + off;
                uid2id_.emplace(absl::string_view(ptr, static_cast<size_t>(len)), vid);
            } else if (uid_lstr && !uid_lstr->IsNull(i)) {
                const int64_t off = uid_lstr->value_offset(i);
                const int64_t len = uid_lstr->value_length(i);
                const char*   ptr = reinterpret_cast<const char*>(uid_lstr->value_data()->data()) + off;
                uid2id_.emplace(absl::string_view(ptr, static_cast<size_t>(len)), vid);
            }
        }
    }
    uid_map_ready_ = !uid2id_.empty();
}
void EnhancedGraph::add_edges_from_arrow(const std::shared_ptr<arrow::Table>& table) {
    // ---------- PASS 0: find max vertex id for dense degree arrays ----------
    VertexId max_id = 0;
    for (const auto& kv : vertices_) if (kv.first > max_id) max_id = kv.first;
    const size_t N = static_cast<size_t>(max_id) + 1;

    // Build a temporary dense index id -> Vertex* to avoid billions of hash lookups.
    // Scope-limited so it frees immediately after ingestion.
    std::vector<Vertex*> id2v(N, nullptr);
    id2v.shrink_to_fit(); // allow tight capacity without over-alloc
    for (const auto& kv : vertices_) {
        id2v[kv.first] = kv.second;
    }

    // ---------- PASS 1: count degrees to pre-reserve exact capacities ----------
    std::vector<uint32_t> out_deg(N, 0), in_deg(N, 0);
    
    // Edge filtering statistics
    size_t skipped_null = 0, skipped_oob = 0, skipped_missing = 0, processed_edges = 0;

    auto count_batch = [&](const std::shared_ptr<arrow::RecordBatch>& batch) {
        auto source_col = batch->GetColumnByName("source_id");
        auto target_col = batch->GetColumnByName("target_id");
        auto source_uid_col = (!source_col ? batch->GetColumnByName("source_uid") : nullptr);
        auto target_uid_col = (!target_col ? batch->GetColumnByName("target_uid") : nullptr);
        if (!source_col && !source_uid_col) throw std::runtime_error("edges: missing source_id/source_uid");
        if (!target_col && !target_uid_col) throw std::runtime_error("edges: missing target_id/target_uid");

        if (source_col && target_col) {
            std::shared_ptr<arrow::UInt32Array> src_u32; std::shared_ptr<arrow::Int64Array> src_i64; std::shared_ptr<arrow::Int32Array> src_i32;
            std::shared_ptr<arrow::UInt32Array> tgt_u32; std::shared_ptr<arrow::Int64Array> tgt_i64; std::shared_ptr<arrow::Int32Array> tgt_i32;
            switch (source_col->type()->id()) {
                case arrow::Type::UINT32: src_u32 = std::static_pointer_cast<arrow::UInt32Array>(source_col); break;
                case arrow::Type::INT64:  src_i64 = std::static_pointer_cast<arrow::Int64Array>(source_col); break;
                case arrow::Type::INT32:  src_i32 = std::static_pointer_cast<arrow::Int32Array>(source_col); break;
                default: throw std::runtime_error("Unsupported source_id column type " + source_col->type()->ToString());
            }
            switch (target_col->type()->id()) {
                case arrow::Type::UINT32: tgt_u32 = std::static_pointer_cast<arrow::UInt32Array>(target_col); break;
                case arrow::Type::INT64:  tgt_i64 = std::static_pointer_cast<arrow::Int64Array>(target_col); break;
                case arrow::Type::INT32:  tgt_i32 = std::static_pointer_cast<arrow::Int32Array>(target_col); break;
                default: throw std::runtime_error("Unsupported target_id column type " + target_col->type()->ToString());
            }
                    for (int64_t i = 0; i < batch->num_rows(); ++i) {
            // NULL guard
            if ((src_u32 && src_u32->IsNull(i)) || (src_i64 && src_i64->IsNull(i)) || (src_i32 && src_i32->IsNull(i)) ||
                (tgt_u32 && tgt_u32->IsNull(i)) || (tgt_i64 && tgt_i64->IsNull(i)) || (tgt_i32 && tgt_i32->IsNull(i))) {
                ++skipped_null;
                continue;
            }

            VertexId s = src_i64 ? static_cast<VertexId>(src_i64->Value(i))
                        : src_i32 ? static_cast<VertexId>(src_i32->Value(i))
                                  : static_cast<VertexId>(src_u32->Value(i));
            VertexId t = tgt_i64 ? static_cast<VertexId>(tgt_i64->Value(i))
                        : tgt_i32 ? static_cast<VertexId>(tgt_i32->Value(i))
                                  : static_cast<VertexId>(tgt_u32->Value(i));
            if (flip_edge_direction_on_ingest_) std::swap(s, t);

            // BOUNDS + EXISTENCE guard
            if (s >= N || t >= N) {
                ++skipped_oob;
                continue;
            }
            if (!id2v[s] || !id2v[t]) {
                ++skipped_missing;
                continue;
            }
            ++out_deg[s]; ++in_deg[t];
            ++processed_edges;
        }
        } else {
            if (!uid_map_ready_) throw std::runtime_error("UID mapping required but not built (no UID column seen in vertices)");
            std::shared_ptr<arrow::StringArray> su, tu; std::shared_ptr<arrow::LargeStringArray> slu, tlu;
            if (source_uid_col->type()->id() == arrow::Type::STRING) su = std::static_pointer_cast<arrow::StringArray>(source_uid_col);
            else                                                     slu = std::static_pointer_cast<arrow::LargeStringArray>(source_uid_col);
            if (target_uid_col->type()->id() == arrow::Type::STRING) tu = std::static_pointer_cast<arrow::StringArray>(target_uid_col);
            else                                                     tlu = std::static_pointer_cast<arrow::LargeStringArray>(target_uid_col);
            auto view_at = [](const std::shared_ptr<arrow::StringArray>& arr, int64_t i){ 
                const int32_t off = arr->value_offset(static_cast<int32_t>(i));
                const int32_t len = arr->value_length(static_cast<int32_t>(i));
                const char*   ptr = reinterpret_cast<const char*>(arr->value_data()->data()) + off;
                return absl::string_view(ptr, static_cast<size_t>(len)); };
            auto lview_at = [](const std::shared_ptr<arrow::LargeStringArray>& arr, int64_t i){ 
                const int64_t off = arr->value_offset(i);
                const int64_t len = arr->value_length(i);
                const char*   ptr = reinterpret_cast<const char*>(arr->value_data()->data()) + off;
                return absl::string_view(ptr, static_cast<size_t>(len)); };
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if ((su && su->IsNull(i)) || (slu && slu->IsNull(i)) ||
                    (tu && tu->IsNull(i)) || (tlu && tlu->IsNull(i))) {
                    ++skipped_null;
                    continue;
                }
                auto sid = su ? view_at(su, i) : lview_at(slu, i);
                auto tid = tu ? view_at(tu, i) : lview_at(tlu, i);
                auto si = uid2id_.find(sid); 
                auto ti = uid2id_.find(tid);
                if (si == uid2id_.end() || ti == uid2id_.end()) {
                    ++skipped_missing;
                    continue;
                }
                VertexId s = si->second, t = ti->second;
                if (flip_edge_direction_on_ingest_) std::swap(s, t);
                
                // BOUNDS + EXISTENCE guard
                if (s >= N || t >= N) {
                    ++skipped_oob;
                    continue;
                }
                if (!id2v[s] || !id2v[t]) {
                    ++skipped_missing;
                    continue;
                }
                ++out_deg[s]; ++in_deg[t];
                ++processed_edges;
            }
        }
    };

    // First pass over table (count degrees)
    {
        arrow::TableBatchReader reader1(*table);
        reader1.set_chunksize(get_ingest_chunk_size());
        std::shared_ptr<arrow::RecordBatch> batch;
        while (true) {
            auto next = reader1.Next();
            if (!next.ok()) throw std::runtime_error(next.status().message());
            batch = *next;
            if (!batch) break;
            count_batch(batch);
        }
    }

    // Edge filtering statistics available but not logged for performance

    // Reserve space exactly once per vertex / key (use id2v for O(1) access)
    size_t nonzero_in = 0;
    for (VertexId v = 0; v < N; ++v) {
        if (out_deg[v]) if (Vertex* p = id2v[v]) p->outgoing_edges.reserve(out_deg[v]);
        if (in_deg[v]) ++nonzero_in;
    }
    incoming_edges_.reserve(nonzero_in);
    for (VertexId v = 0; v < N; ++v) {
        if (in_deg[v]) incoming_edges_[v].reserve(in_deg[v]);
    }

    // ---------- PASS 2: actually add edges (now allocations are O(1)) ----------
    // Per-vertex parity check: track actual pushes vs expected in_deg
    std::vector<uint32_t> pushed_in(N, 0);
    
    auto add_batch = [&](const std::shared_ptr<arrow::RecordBatch>& batch) {
        auto source_col = batch->GetColumnByName("source_id");
        auto target_col = batch->GetColumnByName("target_id");
        auto source_uid_col = (!source_col ? batch->GetColumnByName("source_uid") : nullptr);
        auto target_uid_col = (!target_col ? batch->GetColumnByName("target_uid") : nullptr);
        if (!source_col && !source_uid_col) throw std::runtime_error("edges: missing source_id/source_uid");
        if (!target_col && !target_uid_col) throw std::runtime_error("edges: missing target_id/target_uid");

        if (source_col && target_col) {
            std::shared_ptr<arrow::UInt32Array> src_u32; std::shared_ptr<arrow::Int64Array> src_i64; std::shared_ptr<arrow::Int32Array> src_i32;
            std::shared_ptr<arrow::UInt32Array> tgt_u32; std::shared_ptr<arrow::Int64Array> tgt_i64; std::shared_ptr<arrow::Int32Array> tgt_i32;
            switch (source_col->type()->id()) {
                case arrow::Type::UINT32: src_u32 = std::static_pointer_cast<arrow::UInt32Array>(source_col); break;
                case arrow::Type::INT64:  src_i64 = std::static_pointer_cast<arrow::Int64Array>(source_col); break;
                case arrow::Type::INT32:  src_i32 = std::static_pointer_cast<arrow::Int32Array>(source_col); break;
                default: throw std::runtime_error("Unsupported source_id column type " + source_col->type()->ToString());
            }
            switch (target_col->type()->id()) {
                case arrow::Type::UINT32: tgt_u32 = std::static_pointer_cast<arrow::UInt32Array>(target_col); break;
                case arrow::Type::INT64:  tgt_i64 = std::static_pointer_cast<arrow::Int64Array>(target_col); break;
                case arrow::Type::INT32:  tgt_i32 = std::static_pointer_cast<arrow::Int32Array>(target_col); break;
                default: throw std::runtime_error("Unsupported target_id column type " + target_col->type()->ToString());
            }
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                // NULL guard
                if ((src_u32 && src_u32->IsNull(i)) || (src_i64 && src_i64->IsNull(i)) || (src_i32 && src_i32->IsNull(i)) ||
                    (tgt_u32 && tgt_u32->IsNull(i)) || (tgt_i64 && tgt_i64->IsNull(i)) || (tgt_i32 && tgt_i32->IsNull(i))) {
                    continue;
                }

                VertexId s = src_i64 ? static_cast<VertexId>(src_i64->Value(i))
                            : src_i32 ? static_cast<VertexId>(src_i32->Value(i))
                                      : static_cast<VertexId>(src_u32->Value(i));
                VertexId t = tgt_i64 ? static_cast<VertexId>(tgt_i64->Value(i))
                            : tgt_i32 ? static_cast<VertexId>(tgt_i32->Value(i))
                                      : static_cast<VertexId>(tgt_u32->Value(i));
                if (flip_edge_direction_on_ingest_) std::swap(s, t);
                
                // BOUNDS + EXISTENCE guard  
                if (s >= N || t >= N) continue;
                Vertex* sp = id2v[s];
                Vertex* tp = id2v[t];
                if (!sp || !tp) continue;
                Edge* e = new Edge(sp, tp);
                sp->outgoing_edges.push_back(e);
                incoming_edges_[t].push_back(sp);
                ++pushed_in[t];  // Track actual push for parity check
            }
        } else {
            if (!uid_map_ready_) throw std::runtime_error("UID mapping required but not built (no UID column seen in vertices)");
            std::shared_ptr<arrow::StringArray> su, tu; std::shared_ptr<arrow::LargeStringArray> slu, tlu;
            if (source_uid_col->type()->id() == arrow::Type::STRING) su = std::static_pointer_cast<arrow::StringArray>(source_uid_col);
            else                                                     slu = std::static_pointer_cast<arrow::LargeStringArray>(source_uid_col);
            if (target_uid_col->type()->id() == arrow::Type::STRING) tu = std::static_pointer_cast<arrow::StringArray>(target_uid_col);
            else                                                     tlu = std::static_pointer_cast<arrow::LargeStringArray>(target_uid_col);
            auto view_at = [](const std::shared_ptr<arrow::StringArray>& arr, int64_t i){ 
                const int32_t off = arr->value_offset(static_cast<int32_t>(i));
                const int32_t len = arr->value_length(static_cast<int32_t>(i));
                const char*   ptr = reinterpret_cast<const char*>(arr->value_data()->data()) + off;
                return absl::string_view(ptr, static_cast<size_t>(len)); };
            auto lview_at = [](const std::shared_ptr<arrow::LargeStringArray>& arr, int64_t i){ 
                const int64_t off = arr->value_offset(i);
                const int64_t len = arr->value_length(i);
                const char*   ptr = reinterpret_cast<const char*>(arr->value_data()->data()) + off;
                return absl::string_view(ptr, static_cast<size_t>(len)); };
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if ((su && su->IsNull(i)) || (slu && slu->IsNull(i)) ||
                    (tu && tu->IsNull(i)) || (tlu && tlu->IsNull(i))) continue;
                auto sid = su ? view_at(su, i) : lview_at(slu, i);
                auto tid = tu ? view_at(tu, i) : lview_at(tlu, i);
                auto si = uid2id_.find(sid); if (si == uid2id_.end()) continue;
                auto ti = uid2id_.find(tid); if (ti == uid2id_.end()) continue;
                VertexId s = si->second;
                VertexId t = ti->second;
                if (flip_edge_direction_on_ingest_) std::swap(s, t);
                Vertex* sp = (s < N) ? id2v[s] : nullptr;
                Vertex* tp = (t < N) ? id2v[t] : nullptr;
                if (!sp || !tp) continue;
                Edge* e = new Edge(sp, tp);
                sp->outgoing_edges.push_back(e);
                incoming_edges_[t].push_back(sp);
                ++pushed_in[t];  // Track actual push for parity check (UID path)
            }
        }
    };

    // Second pass over table (materialize edges)
    {
        arrow::TableBatchReader reader2(*table);
        reader2.set_chunksize(get_ingest_chunk_size());
        std::shared_ptr<arrow::RecordBatch> batch;
        while (true) {
            auto next = reader2.Next();
            if (!next.ok()) throw std::runtime_error(next.status().message());
            batch = *next;
            if (!batch) break;
            add_batch(batch);
        }
    }
    
    // Note: Parity verification removed for performance (was O(N) per batch)
}

void EnhancedGraph::clear_uid_map() {
    uid2id_.clear();
    uid2id_.rehash(0);
    uid_owner_chunks_.clear(); // release pinned Arrow buffers
    uid_owner_chunks_.shrink_to_fit();
    uid_map_ready_ = false;
}

void EnhancedGraph::clear_predecessor_cache() {
    std::lock_guard<std::mutex> l(predecessor_mutex_);
    predecessor_bitmap_cache_.clear();
    predecessor_bitmap_cache_unfiltered_.clear();
    
    std::lock_guard<std::mutex> lk(bany_mu_);
    bany_lru_.clear();
}

std::shared_ptr<const Roaring> EnhancedGraph::get_cached_predecessor_bitmap_sp(VertexId pred_id, timestamp_t focal_time, time_delta_t dt) {
    std::lock_guard<std::mutex> l(predecessor_mutex_);
    
    // Use composite key including focal_time and dt for correct caching
    PredWinKey key{pred_id, focal_time, dt};
    ++g_benchmark.hf_all;  // Track cache access
    if (auto sp = predecessor_bitmap_cache_.get_copy(key)) {
        ++g_benchmark.hf_hit;  // Track cache hit
        return sp;  // Cache hit - return shared_ptr copy
    }
    
    // Cache miss - compute and store
    auto result = std::make_shared<Roaring>();
    auto edge_it = incoming_edges_.find(pred_id);
    if (edge_it != incoming_edges_.end()) {
        *result = incoming_edges_sorted_by_time_ ? 
                  build_ref_bitmap_sorted(edge_it->second, focal_time, dt) :
                  build_ref_bitmap(edge_it->second, focal_time, dt);
    }
    
    // LRU handles eviction automatically, return shared_ptr copy
    return predecessor_bitmap_cache_.put_and_get_copy(key, result);
}
        
std::shared_ptr<const Roaring> EnhancedGraph::get_cached_predecessor_bitmap_unfiltered_sp(VertexId pred_id) {
    std::lock_guard<std::mutex> l(predecessor_mutex_);
    ++g_benchmark.hu_all;  // Track cache access
    if (auto sp = predecessor_bitmap_cache_unfiltered_.get_copy(pred_id)) {
        ++g_benchmark.hu_hit;  // Track cache hit
        return sp;  // Cache hit - return shared_ptr copy
    }
    
    auto result = std::make_shared<Roaring>();
    auto in_it = incoming_edges_.find(pred_id);
    if (in_it != incoming_edges_.end()) {
        for (const Vertex* citer : in_it->second) result->add(citer->id);
    }
    
    // LRU handles eviction automatically, return shared_ptr copy
    return predecessor_bitmap_cache_unfiltered_.put_and_get_copy(pred_id, result);
}

std::shared_ptr<const Roaring> EnhancedGraph::get_bany_for_focal_sp(VertexId fid) const {
    std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(bany_mu_));
    ++g_benchmark.hb_all;  // Track cache access
    if (auto sp = const_cast<EnhancedGraph*>(this)->bany_lru_.get_copy(fid)) {
        ++g_benchmark.hb_hit;  // Track cache hit
        return sp;  // Cache hit - return shared_ptr copy
    }
    
    // Build B_any = union of all unfiltered citers of focal's predecessors
    auto r = std::make_shared<Roaring>();
    auto focal_it = vertices_.find(fid);
    if (focal_it != vertices_.end()) {
        for (const auto* e : focal_it->second->outgoing_edges) {
            auto in_it = incoming_edges_.find(e->target->id);
            if (in_it != incoming_edges_.end())
                for (const Vertex* citer : in_it->second) r->add(citer->id);
        }
    }
    
    // Optimize large B_any bitmaps (cost/benefit threshold)
    if (r->cardinality() > 100000) {
        r->runOptimize();
    }
    
    // LRU handles eviction automatically, return shared_ptr copy
    return const_cast<EnhancedGraph*>(this)->bany_lru_.put_and_get_copy(fid, r);
}

std::shared_ptr<const Roaring> EnhancedGraph::get_window_bitmap_sp(timestamp_t ft, time_delta_t dt) const {
    std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(timewin_mu_));
    ++g_benchmark.hw_all;  // Track time window cache access
    
    TimeWinKey key{ft, dt};
    if (auto sp = const_cast<EnhancedGraph*>(this)->timewin_lru_.get_copy(key)) {
        ++g_benchmark.hw_hit;  // Track cache hit
        return sp;  // Cache hit - return shared_ptr copy
    }

    // Build W_t by unioning only the needed years (no giant prefix arrays!)
    // CONSISTENCY CHECK: Ensure F_t uses Vertex::time and W_t uses PropertyStore["year"]
    // from the same year column during ingestion (both should represent publication year)
    
    // Type and boundary checks for timestamp_t to int conversion
    constexpr int64_t MAX_YEAR = 2147483647;  // max int32
    constexpr int64_t MIN_YEAR = -2147483648; // min int32
    
    int64_t start_year_64 = static_cast<int64_t>(ft);
    int64_t end_year_64 = static_cast<int64_t>(ft + dt);
    
    // Clamp to valid int32 range for year indexing
    int start_year = static_cast<int>(std::max(MIN_YEAR, std::min(MAX_YEAR, start_year_64)));
    int end_year = static_cast<int>(std::max(MIN_YEAR, std::min(MAX_YEAR, end_year_64)));
    
    auto W = std::make_shared<Roaring>(properties.get_window_bitmap_by_union("year", start_year, end_year));
    
    // LRU handles eviction automatically, return shared_ptr copy
    return const_cast<EnhancedGraph*>(this)->timewin_lru_.put_and_get_copy(key, W);
}


void EnhancedGraph::build_region_bitmaps_from(PropertyStore& props,
    const CountryLists& lists) {
    regions_.us = props.get_combined_bitmap_str("country", lists.us_names);
    regions_.cn = props.get_combined_bitmap_str("country", lists.cn_names);
    regions_.eu = props.get_combined_bitmap_str("country", lists.eu_names);
    regions_.us.runOptimize(); regions_.cn.runOptimize(); regions_.eu.runOptimize();
}

void EnhancedGraph::set_country_lists(CountryLists lists) {
    country_lists_ = std::move(lists);
}

void EnhancedGraph::set_country_lists_by_names(const std::vector<std::string>& us,
                                               const std::vector<std::string>& cn,
                                               const std::vector<std::string>& eu) {
    CountryLists lists;
    lists.us_names = us; lists.cn_names = cn; lists.eu_names = eu;
    set_country_lists(std::move(lists));
}

// ---- Region bitmap persistence ----
static bool write_roar(const Roaring& r, const std::string& path) {
    FILE* f = fopen(path.c_str(), "wb"); if (!f) return false;
    size_t sz = r.getSizeInBytes();
    std::string buf; buf.resize(sz);
    r.write(reinterpret_cast<char*>(buf.data()));
    size_t w = fwrite(buf.data(), 1, sz, f);
    fclose(f);
    return w == sz;
}
static bool read_roar(Roaring& r, const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb"); if (!f) return false;
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0) { fclose(f); return false; }
    std::string buf; buf.resize(sz);
    size_t rd = fread(buf.data(), 1, sz, f);
    fclose(f);
    if (rd != (size_t)sz) return false;
    r = Roaring::read(reinterpret_cast<const char*>(buf.data()));
    return true;
}

bool EnhancedGraph::save_region_bitmaps(const std::string& dir) const {
    std::string d = dir; if (!d.empty() && d.back() != '/') d.push_back('/');
    // ensure compact on write (optional)
    const_cast<Roaring&>(regions_.us).runOptimize();
    const_cast<Roaring&>(regions_.cn).runOptimize();
    const_cast<Roaring&>(regions_.eu).runOptimize();
    
    std::cerr << "[regions] Saving with enhanced validation to " << d << std::endl;
    return write_roar_safe(regions_.us, d + "us.roar")
        && write_roar_safe(regions_.cn, d + "cn.roar")
        && write_roar_safe(regions_.eu, d + "eu.roar");
}
bool EnhancedGraph::load_region_bitmaps(const std::string& dir) {
    std::string d = dir; if (!d.empty() && d.back() != '/') d.push_back('/');
    Roaring us, cn, eu;
    
    std::cerr << "[regions] Loading with enhanced validation from " << d << std::endl;
    
    // Try safe read with validation
    bool us_ok = read_roar_safe(us, d + "us.roar");
    if (!us_ok) {
        std::cerr << "[regions] Safe read failed for us.roar, trying basic read..." << std::endl;
        us_ok = read_roar(us, d + "us.roar");
    }
    if (!us_ok) return false;
    
    bool cn_ok = read_roar_safe(cn, d + "cn.roar");
    if (!cn_ok) {
        std::cerr << "[regions] Safe read failed for cn.roar, trying basic read..." << std::endl;
        cn_ok = read_roar(cn, d + "cn.roar");
    }
    if (!cn_ok) return false;
    
    bool eu_ok = read_roar_safe(eu, d + "eu.roar");
    if (!eu_ok) {
        std::cerr << "[regions] Safe read failed for eu.roar, trying basic read..." << std::endl;
        eu_ok = read_roar(eu, d + "eu.roar");
    }
    if (!eu_ok) return false;
    regions_.us = std::move(us);
    regions_.cn = std::move(cn);
    regions_.eu = std::move(eu);
    // Mark as prebuilt so region_bitmap_for() will NOT rebuild/overwrite from PropertyStore.
    regions_prebuilt_ = true;
    return true;
}

std::tuple<uint64_t,uint64_t,uint64_t> EnhancedGraph::region_sizes() const {
    return { regions_.us.cardinality(), regions_.eu.cardinality(), regions_.cn.cardinality() };
}

// ---- PropertyStore persistence for "year" ----
bool PropertyStore::save_bitmaps(const std::string& prop, const std::string& dir) const {
    auto it = categorical_bitmaps_.find(prop);
    if (it == categorical_bitmaps_.end()) return false;
    std::string d = dir; if (!d.empty() && d.back() != '/') d.push_back('/');
    // mkdir -p is left to caller
    for (const auto& kv : it->second) {
        std::string path = d + prop + "_" + std::to_string(kv.first) + ".roar";
        if (!write_roar_safe(kv.second, path)) return false;
    }
    return true;
}
bool PropertyStore::load_bitmaps(const std::string& prop, const std::string& dir) {
    // very simple loader: scan directory for prop_*.roar
    std::string d = dir; if (!d.empty() && d.back() != '/') d.push_back('/');
    // assume caller knows which years exist; try a reasonable range or pre-generated index.
    // Minimal: try to open files present in directory via POSIX glob would be best; here we keep it minimal.
    // If you need robust loading, add a manifest. For now, return false to avoid surprising behavior.
    return false;
}

const Roaring* EnhancedGraph::region_bitmap_for(CiterFilter f) const {
    // std::call_once(regions_once_, [&]{
    //     // Build on first use, thread-safe
    //     // Note: PropertyStore is immutable at query time here
    //     const_cast<EnhancedGraph*>(this)->build_region_bitmaps_from(
    //         const_cast<PropertyStore&>(this->properties), country_lists_);
    // });
    if (!regions_prebuilt_) {
        // If bitmaps were populated via load_region_bitmaps() but the flag wasn't set,
        // detect non-empty bitmaps and just flip the flag to avoid overwriting.
        if (!(regions_.us.isEmpty() && regions_.eu.isEmpty() && regions_.cn.isEmpty())) {
            const_cast<EnhancedGraph*>(this)->regions_prebuilt_ = true;
        } else {
            std::call_once(regions_once_, [&]{
                if (!regions_prebuilt_) {
                    const_cast<EnhancedGraph*>(this)->build_region_bitmaps_from(
                        const_cast<PropertyStore&>(this->properties), country_lists_);
                    const_cast<EnhancedGraph*>(this)->regions_prebuilt_ = true;
                }
            });
        }
    }
    switch (f) {
        case CiterFilter::OnlyUS:
        case CiterFilter::ExcludeUS: return &regions_.us;
        case CiterFilter::OnlyCN:
        case CiterFilter::ExcludeCN: return &regions_.cn;
        case CiterFilter::OnlyEU:
        case CiterFilter::ExcludeEU: return &regions_.eu;
        default: return nullptr;
    }
}

arrow::Status EnhancedGraph::ingest_countries_from_parquet(const std::shared_ptr<arrow::Table>& table,
                                                           const std::string& uid_col,
                                                           const std::string& country_col) {

    if (!table) return arrow::Status::Invalid("null table for ingest_countries_from_parquet");

    // Pre-build fast membership sets (tiny)
    std::unordered_set<std::string_view> us_set, cn_set, eu_set;
    us_set.reserve(country_lists_.us_names.size());
    cn_set.reserve(country_lists_.cn_names.size());
    eu_set.reserve(country_lists_.eu_names.size());
    for (const auto& s : country_lists_.us_names) us_set.emplace(std::string_view{s.data(), s.size()});
    for (const auto& s : country_lists_.cn_names) cn_set.emplace(std::string_view{s.data(), s.size()});
    for (const auto& s : country_lists_.eu_names) eu_set.emplace(std::string_view{s.data(), s.size()});

    auto country_arr_any = table->GetColumnByName(country_col);
    if (!country_arr_any) {
        return arrow::Status::Invalid("country table missing country column: " + country_col);
    }

    // Helper lambdas to make absl::string_view without allocating
    auto sv_at = [](const std::shared_ptr<arrow::StringArray>& arr, int64_t i) -> absl::string_view {
        const int32_t off = arr->value_offset(static_cast<int32_t>(i));
        const int32_t len = arr->value_length(static_cast<int32_t>(i));
        const char*   ptr = reinterpret_cast<const char*>(arr->value_data()->data()) + off;
        return absl::string_view(ptr, static_cast<size_t>(len));
    };
    auto lsv_at = [](const std::shared_ptr<arrow::LargeStringArray>& arr, int64_t i) -> absl::string_view {
        const int64_t off = arr->value_offset(i);
        const int64_t len = arr->value_length(i);
        const char*   ptr = reinterpret_cast<const char*>(arr->value_data()->data()) + off;
        return absl::string_view(ptr, static_cast<size_t>(len));
    };

    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;

    // Fast path if numeric paper_id is present (UINT32/INT64/INT32)
    const auto pid_any = table->GetColumnByName("paper_id");
    const bool have_numeric_pid = (pid_any &&
        (pid_any->type()->id() == arrow::Type::UINT32 ||
         pid_any->type()->id() == arrow::Type::INT64  ||
         pid_any->type()->id() == arrow::Type::INT32));

    while (true) {
        ARROW_ASSIGN_OR_RAISE(batch, reader.Next());
        if (!batch) break;

        auto c_any = batch->GetColumnByName(country_col);
        if (!c_any) continue;
        std::shared_ptr<arrow::StringArray>      c_str;
        std::shared_ptr<arrow::LargeStringArray> c_lstr;
        const auto cid = c_any->type()->id();
        if (cid == arrow::Type::STRING)      c_str  = std::static_pointer_cast<arrow::StringArray>(c_any);
        else if (cid == arrow::Type::LARGE_STRING) c_lstr = std::static_pointer_cast<arrow::LargeStringArray>(c_any);
        else continue; // ignore non-string country column

        if (have_numeric_pid) {
            // Numeric ID + country string
            auto p_any = batch->GetColumnByName("paper_id");
            std::shared_ptr<arrow::UInt32Array> p_u32;
            std::shared_ptr<arrow::Int64Array>  p_i64;
            std::shared_ptr<arrow::Int32Array>  p_i32;
            switch (p_any->type()->id()) {
                case arrow::Type::UINT32: p_u32 = std::static_pointer_cast<arrow::UInt32Array>(p_any); break;
                case arrow::Type::INT64:  p_i64 = std::static_pointer_cast<arrow::Int64Array>(p_any); break;
                case arrow::Type::INT32:  p_i32 = std::static_pointer_cast<arrow::Int32Array>(p_any); break;
                default: break;
            }
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if ((c_str  && c_str->IsNull(i))  || (c_lstr && c_lstr->IsNull(i))) continue;
                VertexId vid;
                if      (p_u32 && !p_u32->IsNull(i)) vid = p_u32->Value(i);
                else if (p_i64 && !p_i64->IsNull(i)) vid = static_cast<VertexId>(p_i64->Value(i));
                else if (p_i32 && !p_i32->IsNull(i)) vid = static_cast<VertexId>(p_i32->Value(i));
                else continue;

                absl::string_view c = c_str ? sv_at(c_str, i) : lsv_at(c_lstr, i);
                const std::string_view cv{c.data(), c.size()};
                if      (us_set.count(cv)) regions_.us.add(vid);
                else if (cn_set.count(cv)) regions_.cn.add(vid);
                else if (eu_set.count(cv)) regions_.eu.add(vid);
            }
        } else {
            // UID + country string: zero-copy lookup via uid2id_ (keys are string_view)
            auto u_any = batch->GetColumnByName(uid_col);
            if (!u_any) continue;
            std::shared_ptr<arrow::StringArray>      u_str;
            std::shared_ptr<arrow::LargeStringArray> u_lstr;
            const auto uid = u_any->type()->id();
            if (uid == arrow::Type::STRING)      u_str  = std::static_pointer_cast<arrow::StringArray>(u_any);
            else if (uid == arrow::Type::LARGE_STRING) u_lstr = std::static_pointer_cast<arrow::LargeStringArray>(u_any);
            else continue;

            if (!uid_map_ready_) {
                return arrow::Status::Invalid("UID mapping required but not built: vertices must be ingested first with UID column");
            }
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if ((c_str && c_str->IsNull(i)) || (c_lstr && c_lstr->IsNull(i))) continue;
                if ((u_str && u_str->IsNull(i)) || (u_lstr && u_lstr->IsNull(i))) continue;

                absl::string_view uid_view = u_str ? sv_at(u_str, i) : lsv_at(u_lstr, i);
                auto it = uid2id_.find(uid_view);
                if (it == uid2id_.end()) continue;
                VertexId vid = it->second;

                absl::string_view c = c_str ? sv_at(c_str, i) : lsv_at(c_lstr, i);
                const std::string_view cv{c.data(), c.size()};
                if      (us_set.count(cv)) regions_.us.add(vid);
                else if (cn_set.count(cv)) regions_.cn.add(vid);
                else if (eu_set.count(cv)) regions_.eu.add(vid);
            }
        }
    }

    regions_.us.runOptimize();
    regions_.cn.runOptimize();
    regions_.eu.runOptimize();
    regions_prebuilt_ = true;  // tell region_bitmap_for() not to rebuild from PropertyStore
    return arrow::Status::OK();
}

bool EnhancedGraph::debug_check_bany(VertexId fid) const {
    auto it = vertices_.find(fid);
    if (it == vertices_.end()) return false;
    const Vertex* focal = it->second;
    
    // Fallback build: citers of focal's references (union over incoming of each reference)
    Roaring fallback;
    for (const Edge* e : focal->outgoing_edges) {
        VertexId ref = e->target->id;
        auto in_it = incoming_edges_.find(ref);
        if (in_it == incoming_edges_.end()) continue;
        const auto& citers = in_it->second;
        for (const Vertex* c : citers) fallback.add(c->id);
    }

        // Cached
    auto cached_sp = get_bany_for_focal_sp(fid);
    if (!cached_sp) return false;
    const Roaring& cached = *cached_sp;

    // Compare via XOR emptiness (safe for roaring)
    Roaring diff = cached ^ fallback;
    return diff.isEmpty();
}

// --- Debug helper: return (|F_t|, |F_t∩B_any|, |B_any∩W_t|, denom, cd) ---
std::tuple<uint64_t,uint64_t,uint64_t,uint64_t,double>
EnhancedGraph::debug_counts(VertexId fid, time_delta_t dt) {
    auto fit = vertices_.find(fid);
    if (fit == vertices_.end()) {
        return {0,0,0,0,std::numeric_limits<double>::quiet_NaN()};
    }
    const timestamp_t ft = fit->second->time;
    Roaring F_t;
    if (auto it = incoming_edges_.find(fid); it != incoming_edges_.end()) {
        F_t = incoming_edges_sorted_by_time_ ?
              build_ref_bitmap_sorted(it->second, ft, dt) :
              build_ref_bitmap(it->second, ft, dt);
    }
    auto B_any = get_bany_for_focal_sp(fid);
    auto W_t   = get_window_bitmap_sp(ft, dt);
    const uint64_t cF   = F_t.cardinality();
    const uint64_t cFB  = and_cardinality_safe(F_t, *B_any);
    const uint64_t bwin = and_cardinality_safe(*B_any, *W_t);
    const uint64_t denom = cF + bwin - cFB;
    const double cd = denom ? (double(cF) - 2.0*double(cFB)) / double(denom)
                            : std::numeric_limits<double>::quiet_NaN();
#ifndef NDEBUG
    // Parity: |F ∪ (B_any∩W)| must equal cF + bwin − cFB
    Roaring U = F_t; U |= (*B_any & *W_t);
    assert(U.cardinality() == denom);
#endif
    return {cF, cFB, bwin, denom, cd};
}