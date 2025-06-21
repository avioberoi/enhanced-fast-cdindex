#include "cdindex_enhanced.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <roaring/roaring.hh>
#include <absl/container/flat_hash_map.h>
#include <omp.h>
#include <mutex>

static int64_t get_env_int(const char* var, int64_t def) {
    if (auto e = std::getenv(var)) try { auto v = std::stoll(e); if (v > 0) return v; } catch(...) {}
    return def;
}
int64_t BATCH_PARALLEL_THRESHOLD() { return get_env_int("BATCH_PARALLEL_THRESHOLD", 10000); }
int64_t INNER_PARALLEL_THRESHOLD() { return get_env_int("INNER_PARALLEL_THRESHOLD", 1000); }
int64_t MAX_CACHE_ENTRIES() { return get_env_int("MAX_CACHE_ENTRIES", 8); }
int64_t CHUNK_SIZE() { return get_env_int("CHUNK_SIZE", 1000000); }
int64_t get_ingest_chunk_size() { return get_env_int("INGEST_CHUNK_SIZE", 1000000); }

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

    std::cerr << "PropertyStore::ingest_arrow: table rows=" << table->num_rows()
              << ", columns=" << table->num_columns()
              << ", chunk_size=" << get_ingest_chunk_size() << std::endl;
    std::cerr << "PropertyStore::ingest_arrow: columns schema: ";
    for (int col_i = 0; col_i < table->num_columns(); ++col_i) {
        auto field = table->schema()->field(col_i);
        std::cerr << field->name() << "(" << field->type()->ToString() << ") ";
    }
    std::cerr << std::endl;

    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;

    while (true) {
        auto next_batch = reader.Next();
        if (!next_batch.ok()) return next_batch.status();
        batch = *next_batch;
        if (!batch) break;

        auto ids_array = std::static_pointer_cast<arrow::UInt32Array>(batch->GetColumnByName("paper_id"));

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
                        add_categorical(ids_array->Value(j), col_name, int_array->Value(j));
                    }
                }
                } else {
                    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(col);
                    for (int64_t j = 0; j < col->length(); ++j) {
                        if (!col->IsNull(j)) {
                            add_categorical(ids_array->Value(j), col_name, static_cast<int64_t>(int32_array->Value(j)));
                        }
                    }
                }
            } else if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING) {
                auto str_array = std::static_pointer_cast<arrow::StringArray>(col);
                auto& dict = string_dictionaries_[col_name];
                for (int64_t j = 0; j < col->length(); ++j) {
                    if (!col->IsNull(j)) {
                        std::string val = str_array->GetString(j);
                        int code;
                        auto it = dict.find(val);
                        if (it == dict.end()) {
                            code = dict.size() + 1;
                            dict[val] = code;
                        } else {
                            code = it->second;
                        }
                        add_categorical(ids_array->Value(j), col_name, code);
                    }
                }
            } else if (type_id == arrow::Type::BOOL) {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(col);
                for (int64_t j = 0; j < col->length(); ++j) {
                    if (!col->IsNull(j)) {
                        int code = bool_array->Value(j) ? 1 : 0;
                        add_categorical(ids_array->Value(j), col_name, code);
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
    std::cerr << "PropertyStore::ingest_arrow: completed, loaded " << total_entries
              << " property entries across " << categorical_properties_.size()
              << " columns." << std::endl;
    return arrow::Status::OK();
}
void PropertyStore::build_indexes() {
    for (auto const& [prop_name, values] : categorical_properties_) {
        for (auto const& [id, val] : values) {
            categorical_bitmaps_[prop_name][val].add(id);
        }
    }
}
void PropertyStore::add_categorical(VertexId id, const std::string& name, int value) { 
    categorical_properties_[name].emplace_back(id, value);
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
void PropertyStore::clear() { categorical_properties_.clear(); categorical_bitmaps_.clear(); string_dictionaries_.clear();}

static Roaring build_ref_bitmap(const std::vector<Vertex*>& refs, timestamp_t f, time_delta_t d) {
    Roaring r;
    for (auto* v : refs) if (v->time > f && v->time <= f + d) r.add(v->id);
    return r;
}

// Graph methods…
Graph::~Graph() {
    for (auto& kv : vertices_) delete kv.second;
    for (auto* e : all_edges_) delete e;
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
        all_edges_.push_back(new_edge);
    }
}
double Graph::cdindex(VertexId focal_id, time_delta_t dt) { return compute_cdindex_logic(focal_id, get_citers(focal_id, dt), dt); }
double Graph::compute_cdindex_logic(VertexId fid, const std::vector<Vertex*>& citers, time_delta_t dt) {
    auto fit = vertices_.find(fid);
    if (fit == vertices_.end()) return 0.0;
    Vertex* focal = fit->second;
    timestamp_t ft = focal->time;
    Roaring ref_bm = build_ref_bitmap(incoming_edges_[fid], ft, dt);
    for (auto* c : citers) {
        Roaring tmp = build_ref_bitmap(incoming_edges_[c->id], ft, dt);
        ref_bm |= tmp;
    }
    if (ref_bm.isEmpty()) return 0.0;
    Roaring b_bm;
    for (auto* c : citers) for (auto* e : c->outgoing_edges) if (e->target->time > ft && e->target->time <= ft + dt) b_bm.add(e->target->id);
    size_t M = ref_bm.cardinality(); // expensive call: cache cardinality if reused in conditional checks();
    double sum = 0.0;
    bool use_inner = M > INNER_PARALLEL_THRESHOLD();
    #pragma omp parallel for if(use_inner) reduction(+:sum) schedule(dynamic)
    for (size_t i = 0; i < M; ++i) {
        uint32_t vid;
        if (!ref_bm.select(i, &vid)) continue;
        auto it = vertices_.find(vid);
        if (it == vertices_.end()) continue;
        Vertex* v = it->second;
        int f_i = v->has_outgoing_to(fid);
        int b_i = b_bm.contains(vid) ? 1 : 0;
        sum += -2.0 * f_i * b_i + f_i;
    }
    return M ? sum / M : 0.0;
}
void Graph::prepare_for_searching() {
    for (auto& kv : vertices_) {
        kv.second->shrink_to_fit();
        kv.second->sort_outgoing_edges();
    }
}
std::vector<Vertex*> Graph::get_citers(VertexId focal_id, time_delta_t t_delta) {
    // Return all papers that cite the focal paper within the time window
    auto it = vertices_.find(focal_id);
    if (it == vertices_.end()) return {};
    Vertex* focal_paper = it->second;
    timestamp_t focal_time = focal_paper->time;
    std::vector<Vertex*> citers;
    // Outgoing edges for focal are edges from reference to citer, so focal's outgoing_edges list
    for (const auto& edge : focal_paper->outgoing_edges) {
        Vertex* citer = edge->target;
        // Include only citers published after the focal paper, up to t_delta
        if (citer->time > focal_time && citer->time <= focal_time + t_delta) {
            citers.push_back(citer);
        }
    }
    return citers;
}

// EnhancedGraph methods…
void EnhancedGraph::add_vertices_from_arrow(const std::shared_ptr<arrow::Table>& table) {
    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> next_batch = reader.Next();
        if (!next_batch.ok()) {
            throw std::runtime_error(next_batch.status().message());
        }
        batch = *next_batch;
        if (!batch) break;
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
        }
    }
}
void EnhancedGraph::add_edges_from_arrow(const std::shared_ptr<arrow::Table>& table) {
    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(get_ingest_chunk_size());
    std::shared_ptr<arrow::RecordBatch> batch;

    while (true) {
        auto next_batch = reader.Next();
        if (!next_batch.ok()) {
            throw std::runtime_error(next_batch.status().message());
        }
        batch = *next_batch;
        if (!batch) break;

        auto source_col = batch->GetColumnByName("source_id");
        auto target_col = batch->GetColumnByName("target_id");
        // Handle flexible ID types: UINT32, INT64, INT32
        std::shared_ptr<arrow::UInt32Array> src_u32;
        std::shared_ptr<arrow::Int64Array> src_i64;
        std::shared_ptr<arrow::Int32Array> src_i32;
        switch (source_col->type()->id()) {
            case arrow::Type::UINT32:
                src_u32 = std::static_pointer_cast<arrow::UInt32Array>(source_col);
                break;
            case arrow::Type::INT64:
                src_i64 = std::static_pointer_cast<arrow::Int64Array>(source_col);
                break;
            case arrow::Type::INT32:
                src_i32 = std::static_pointer_cast<arrow::Int32Array>(source_col);
                break;
            default:
                throw std::runtime_error("Unsupported source_id column type " + source_col->type()->ToString());
        }
        std::shared_ptr<arrow::UInt32Array> tgt_u32;
        std::shared_ptr<arrow::Int64Array> tgt_i64;
        std::shared_ptr<arrow::Int32Array> tgt_i32;
        switch (target_col->type()->id()) {
            case arrow::Type::UINT32:
                tgt_u32 = std::static_pointer_cast<arrow::UInt32Array>(target_col);
                break;
            case arrow::Type::INT64:
                tgt_i64 = std::static_pointer_cast<arrow::Int64Array>(target_col);
                break;
            case arrow::Type::INT32:
                tgt_i32 = std::static_pointer_cast<arrow::Int32Array>(target_col);
                break;
            default:
                throw std::runtime_error("Unsupported target_id column type " + target_col->type()->ToString());
        }

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            VertexId s;
            if (src_i64) s = static_cast<VertexId>(src_i64->Value(i));
            else if (src_i32) s = static_cast<VertexId>(src_i32->Value(i));
            else s = src_u32->Value(i);
            VertexId t;
            if (tgt_i64) t = static_cast<VertexId>(tgt_i64->Value(i));
            else if (tgt_i32) t = static_cast<VertexId>(tgt_i32->Value(i));
            else t = tgt_u32->Value(i);
            add_edge(s, t);
        }
    }
}
double EnhancedGraph::cdindex_filtered(VertexId focal_id, time_delta_t t_delta, const std::unordered_map<std::string,std::vector<int>>& filters) {
    prepare_for_searching();
    Roaring combined;
    {
        std::lock_guard<std::mutex> l(filter_mutex_);
        if (filter_bitmap_cache_.size() >= MAX_CACHE_ENTRIES()) { filter_bitmap_cache_.clear(); cache_order_.clear(); }
        std::ostringstream key;
        bool init = false;
        for (auto& kv : filters) {
            Roaring inc, exc;
            for (int v : kv.second) {
                if (v >= 0) inc |= properties.get_combined_bitmap(kv.first, {v});
                else exc |= properties.get_combined_bitmap(kv.first, {-v});
            }
            Roaring pb = inc;
            if (!exc.isEmpty()) pb -= exc;
            combined = init ? (combined & pb) : pb;
            init = true;
            key << kv.first << ";";
        }
        std::string k = key.str();
        filter_bitmap_cache_[k] = combined; // TODO: implement true LRU eviction instead of full clear to maintain useful cache entries
        cache_order_.push_back(k);
    }
    auto citers = get_citers(focal_id, t_delta);
    Roaring citer_bm;
    for (auto* c : citers) citer_bm.add(c->id);
    citer_bm &= combined;
    std::vector<Vertex*> filt;
    filt.reserve(citer_bm.cardinality());
    for (auto id : citer_bm) {
        // NOTE: using find() ensures no insertion side-effects; verify performance of hash lookups here
        auto it = vertices_.find(id);
        if (it != vertices_.end()) filt.push_back(it->second);
    }
    return compute_cdindex_logic(focal_id, filt, t_delta);
}
std::shared_ptr<arrow::Table> EnhancedGraph::cdindex_batch(const std::shared_ptr<arrow::UInt32Array>& pids, time_delta_t dt) {
    prepare_for_searching();
    int64_t n = pids->length();
    auto schema = arrow::schema({arrow::field("paper_id", arrow::uint32()), arrow::field("cd5", arrow::float64())});
    if (n <= 0) return arrow::Table::Make(schema, std::vector<std::shared_ptr<arrow::Array>>{});
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    
    // Reuse builders to reduce allocation overhead
    arrow::UInt32Builder ib;
    arrow::DoubleBuilder sb;
    
    for (int64_t off = 0; off < n; off += CHUNK_SIZE()) {
        int64_t sz = std::min(CHUNK_SIZE(), n - off);
        
        // Clear and reserve instead of creating new builders
        ib.Reset();
        sb.Reset();
        ib.Reserve(sz);
        sb.Reserve(sz);
        
        #pragma omp parallel for if(sz > BATCH_PARALLEL_THRESHOLD()) schedule(dynamic)
        // WARNING: Arrow Builders (ib, sb) are not thread-safe; consider collecting local buffers then appending outside the parallel region
        for (int64_t i = 0; i < sz; ++i) {
            uint32_t pid = pids->Value(off + i);
            ib.Append(pid);
            sb.Append(cdindex(pid, dt));
        }
        std::shared_ptr<arrow::Array> ia, sa;
        ib.Finish(&ia);
        sb.Finish(&sa);
        batches.push_back(arrow::RecordBatch::Make(schema, sz, {ia, sa}));
    }
    auto result = arrow::Table::FromRecordBatches(batches);
    return result.ValueOrDie();
}
std::shared_ptr<arrow::Table> EnhancedGraph::cdindex_filtered_batch(const std::shared_ptr<arrow::UInt32Array>& pids, time_delta_t dt, const std::unordered_map<std::string,std::vector<int>>& filters) {
    prepare_for_searching();
    int64_t n = pids->length();
    auto schema = arrow::schema({arrow::field("paper_id", arrow::uint32()), arrow::field("cd5", arrow::float64())});
    if (n <= 0) return arrow::Table::Make(schema, std::vector<std::shared_ptr<arrow::Array>>{});
    Roaring combined;
    {
        std::lock_guard<std::mutex> l(filter_mutex_);
        if (filter_bitmap_cache_.size() >= MAX_CACHE_ENTRIES()) { filter_bitmap_cache_.clear(); cache_order_.clear(); }
        std::ostringstream key;
        bool init = false;
        for (auto& kv : filters) {
            Roaring inc, exc;
            for (int v : kv.second) {
                if (v >= 0) inc |= properties.get_combined_bitmap(kv.first, {v});
                else exc |= properties.get_combined_bitmap(kv.first, {-v});
            }
            Roaring pb = inc;
            if (!exc.isEmpty()) pb -= exc;
            combined = init ? (combined & pb) : pb;
            init = true;
            key << kv.first << ";";
        }
        std::string k = key.str();
        filter_bitmap_cache_[k] = combined; // TODO: implement true LRU eviction instead of full clear to maintain useful cache entries
        cache_order_.push_back(k);
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    
    // Reuse builders to reduce allocation overhead
    arrow::UInt32Builder ib;
    arrow::DoubleBuilder sb;
    
    for (int64_t off = 0; off < n; off += CHUNK_SIZE()) {
        int64_t sz = std::min(CHUNK_SIZE(), n - off);
        
        // Clear and reserve instead of creating new builders
        ib.Reset();
        sb.Reset();
        ib.Reserve(sz);
        sb.Reserve(sz);
        
        #pragma omp parallel for if(sz > BATCH_PARALLEL_THRESHOLD()) schedule(dynamic)
        for (int64_t i = 0; i < sz; ++i) {
            VertexId fid = pids->Value(off + i);
            ib.Append(fid);
            auto citers = get_citers(fid, dt);
            Roaring citer_bm;
            for (auto* c : citers) citer_bm.add(c->id);
            citer_bm &= combined;
            std::vector<Vertex*> filt;
            filt.reserve(citer_bm.cardinality());
            for (auto id : citer_bm) {
            // NOTE: using find() ensures no insertion side-effects; verify performance of hash lookups here
                auto it = vertices_.find(id);
                if (it != vertices_.end()) filt.push_back(it->second);
            }
            sb.Append(compute_cdindex_logic(fid, filt, dt));
        }
        std::shared_ptr<arrow::Array> ia, sa;
        ib.Finish(&ia);
        sb.Finish(&sa);
        batches.push_back(arrow::RecordBatch::Make(schema, sz, {ia, sa}));
    }
    auto result = arrow::Table::FromRecordBatches(batches);
    return result.ValueOrDie();
}
void EnhancedGraph::clear_filter_cache() {
    std::lock_guard<std::mutex> l(filter_mutex_);
    filter_bitmap_cache_.clear();
    cache_order_.clear();
}
