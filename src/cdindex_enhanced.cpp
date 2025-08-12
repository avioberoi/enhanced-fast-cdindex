#include "cdindex_enhanced.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <roaring/roaring.hh>
#include <absl/container/flat_hash_map.h>
#include <omp.h>
#include <mutex>
#include <chrono>
#include <functional>
#include <memory>

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
    std::cerr << "============================================\n\n";
}

// Timing utility for profiling
class Timer {
    std::chrono::high_resolution_clock::time_point start_;
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}
    double elapsed_ms() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }
};

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
                // Pre-reserve dictionary capacity to avoid rehashing
                if (dict.empty()) {
                    dict.reserve(100);  // Estimate based on typical categorical cardinality
                }
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
    // Clear any old data
    categorical_bitmaps_.clear();

    // Pre-allocate one map entry per property (so no concurrent inserts later)
    categorical_bitmaps_.reserve(categorical_properties_.size());
    for (auto const& kv : categorical_properties_) {
        categorical_bitmaps_.emplace(
            kv.first, // property name
            absl::flat_hash_map<int, Roaring>()
        );
    }

    // Gather property names into a vector for simple indexing
    std::vector<std::string> prop_names;
    prop_names.reserve(categorical_properties_.size());
    for (auto const& kv : categorical_properties_) {
        prop_names.push_back(kv.first);
    }

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
        // Single bitmap - just copy
        W = *parts[0];
    } else {
        // Multiple bitmaps - use fastunion with fallback for const issues
        try {
            W = Roaring::fastunion(parts.size(), parts.data());
        } catch (...) {
            // Fallback for potential const mismatch in some CRoaring builds
            for (const auto* p : parts) {
                W |= *p;
            }
        }
    }
    
    // Optimize only for larger unions (small ones don't benefit much)
    if (parts.size() > 2) {
        W.runOptimize();
    }
    
    return W;
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
double Graph::cdindex(VertexId focal_id, time_delta_t dt) { 
    // Pass empty citers vector to build the full correct set in compute_cdindex_logic
    return compute_cdindex_logic(focal_id, {}, dt); 
}
double Graph::compute_cdindex_logic(VertexId fid, const std::vector<Vertex*>& citers, time_delta_t dt) {
    auto fit = vertices_.find(fid);
    if (fit == vertices_.end()) return 0.0;
    Vertex* focal = fit->second;
    timestamp_t ft = focal->time;
    
    // CD Index (Funk & Owen-Smith) tripartite graph structure:
    // 1. Focal patent f (the paper we're analyzing)
    // 2. Predecessors b = {all papers cited by f} (focal->outgoing_edges)
    // 3. Forward-citing patents i = {all papers that cite f OR any b, within time window}
    //
    // For each forward-citing patent i:
    //   f_it = 1 if i cites f, 0 otherwise
    //   b_it = 1 if i cites any predecessor in b, 0 otherwise  
    //   contribution = -2 * f_it * b_it + f_it
    //
    // Cases: +1 = destabilizing (cites only f), -1 = consolidating (cites f+b), 0 = neutral (cites only b)
    
    // Step 1: Build F_t = time-filtered citers of focal
    Roaring F_t;
    {
        ScopedTimer _t1(g_benchmark.t1);
        auto focal_citers_it = incoming_edges_.find(fid);
        if (focal_citers_it != incoming_edges_.end()) {
            F_t = incoming_edges_sorted_by_time_ ? 
                  build_ref_bitmap_sorted(focal_citers_it->second, ft, dt) :
                  build_ref_bitmap(focal_citers_it->second, ft, dt);
        }
    }
    
    // Step 2: Build B_t = time-filtered citers of any predecessor, and B_any = unfiltered citers
    Roaring B_t;
    const Roaring* B_any_ptr = nullptr;
    Roaring B_any_local; // For base Graph fallback
    
    // PERFORMANCE FIX: Detect graph type once, not per-predecessor
    EnhancedGraph* enhanced_graph = dynamic_cast<EnhancedGraph*>(this);
    
    // Step 2: Get B_any reference (cached per focal)
    {
        ScopedTimer _t2(g_benchmark.t2);
        if (enhanced_graph) {
            // Enhanced path: get B_any (no more B_t building!)
            B_any_ptr = &enhanced_graph->get_bany_for_focal(fid);
        } else {
            // Base Graph path: build B_any_local
            for (auto* e : focal->outgoing_edges) {
                VertexId b = e->target->id;
                auto it = incoming_edges_.find(b);
                if (it != incoming_edges_.end()) {
                    // Add to B_any (unfiltered)
                    for (auto* c : it->second) B_any_local.add(c->id);
                }
            }
            B_any_ptr = &B_any_local;
        }
    }
    
    // Step 3: Use window bitmap optimization - no B_t building needed!
    if (citers.empty()) {
        ScopedTimer _t3(g_benchmark.t3);
        ++g_benchmark.n;  // Count this computation
        
        // Get cardinalities for the mathematical reformulation
        uint64_t cF = F_t.cardinality();
        uint64_t cFB = F_t.and_cardinality(*B_any_ptr);
        
        // Compute denominator: |F_t ∪ B_t| = |F_t| + |B_any ∩ W_t| - |F_t ∩ B_any|
        uint64_t denom = 0;
        if (enhanced_graph) {
            // Single intersection with cached small W_t bitmap - much faster!
            const Roaring& W_t = enhanced_graph->get_window_bitmap(ft, dt);
            uint64_t bwin = B_any_ptr->and_cardinality(W_t);     // single intersection
            denom = cF + bwin - cFB;
        } else {
            // Fallback for base Graph: build B_t the old way
            Roaring B_t;
            for (auto* e : focal->outgoing_edges) {
                VertexId b = e->target->id;
                auto it = incoming_edges_.find(b);
                if (it != incoming_edges_.end()) {
                    Roaring filtered = incoming_edges_sorted_by_time_ ? 
                                       build_ref_bitmap_sorted(it->second, ft, dt) :
                                       build_ref_bitmap(it->second, ft, dt);
                    B_t |= filtered;
                }
            }
            denom = F_t.or_cardinality(B_t);
        }
        
        if (denom == 0) return 0.0;
        
        // Numerator stays the same: |F_t| - 2*|F_t ∩ B_any|
        double numerator = static_cast<double>(cF) - 2.0 * static_cast<double>(cFB);
        return numerator / static_cast<double>(denom);
    }
    
    // Handle non-empty citers case (fallback to old approach for now)
    if (!citers.empty()) {
        // For citers filtering, fall back to materializing bitmaps (less common path)
        Roaring I_t = F_t;
        
        // Build B_t properly for this fallback path
        Roaring B_t_fallback;
        if (enhanced_graph) {
            // For the fallback path with citers, we still need to materialize B_t
            // This is less common, so the performance impact is smaller
            const Roaring& W_t = enhanced_graph->get_window_bitmap(ft, dt);
            B_t_fallback = *B_any_ptr & W_t;  // Intersection for time-filtered B_t
        } else {
            // Base Graph fallback: build B_t the old way
            for (auto* e : focal->outgoing_edges) {
                VertexId b = e->target->id;
                auto it = incoming_edges_.find(b);
                if (it != incoming_edges_.end()) {
                    Roaring filtered = incoming_edges_sorted_by_time_ ? 
                                       build_ref_bitmap_sorted(it->second, ft, dt) :
                                       build_ref_bitmap(it->second, ft, dt);
                    B_t_fallback |= filtered;
                }
            }
        }
        I_t |= B_t_fallback;
        
        Roaring citers_bm;
        for (auto* c : citers) citers_bm.add(c->id);
        I_t &= citers_bm;
        F_t &= citers_bm;
        
        if (I_t.isEmpty()) return 0.0;
        
        uint64_t denom = I_t.cardinality();
        uint64_t cF = F_t.cardinality();
        uint64_t cFB = enhanced_graph ? 
            F_t.and_cardinality(enhanced_graph->get_bany_for_focal(fid)) :
            F_t.and_cardinality(B_any_local);
        
        double numerator = static_cast<double>(cF) - 2.0 * static_cast<double>(cFB);
        return numerator / static_cast<double>(denom);
    }
    
    // Should not reach here for the normal case (citers.empty())
    return 0.0;
}

  // Build once from PropertyStore (see helper below) and keep in EnhancedGraph
  
  double Graph::cdindex_filtered(VertexId fid, time_delta_t dt, CiterFilter filt) {
      auto fit = vertices_.find(fid);
      if (fit == vertices_.end()) return 0.0;
      Vertex* focal = fit->second;
      timestamp_t ft = focal->time;
  
      // 1) F_t
      Roaring F_t;
      if (auto it = incoming_edges_.find(fid); it != incoming_edges_.end()) {
          F_t = incoming_edges_sorted_by_time_
              ? build_ref_bitmap_sorted(it->second, ft, dt)
              : build_ref_bitmap(it->second, ft, dt);
      }
  
      // 2) B_any and W_t (by ref; cached)
      EnhancedGraph* eg = dynamic_cast<EnhancedGraph*>(this);
      const Roaring* B_any = nullptr;
      const Roaring* W_t   = nullptr;
      if (eg) {
          B_any = &eg->get_bany_for_focal(fid);
          W_t   = &eg->get_window_bitmap(ft, dt);
      } else {
          // Base fallback: build B_any once (unfiltered)
          // Note: These are allocated on heap to avoid stack overflow for large graphs
          std::unique_ptr<Roaring> tmp_bany = std::make_unique<Roaring>();
          std::unique_ptr<Roaring> tmp_wt = std::make_unique<Roaring>();
          
          for (auto* e : focal->outgoing_edges) {
              if (auto in_it = incoming_edges_.find(e->target->id); in_it != incoming_edges_.end())
                  for (Vertex* citer : in_it->second) tmp_bany->add(citer->id);
          }
          B_any = tmp_bany.get();
          
          // W_t fallback: build time window bitmap (papers in (ft, ft+dt])
          for (const auto& [vid, vertex] : vertices_) {
              if (vertex->time > ft && vertex->time <= ft + dt) {
                  tmp_wt->add(vid);
              }
          }
          W_t = tmp_wt.get();
      }
  
      // 3) Select region bitmap R (US/CN/EU) or empty for None
      const Roaring* R = nullptr;
      if (eg) R = eg->region_bitmap_for(filt);  // returns &us / &cn / &eu, or nullptr
  
      // ---- Cardinalities with filter (no B_t) ----
      uint64_t cF  = F_t.cardinality();           // used for includes/excludes
      uint64_t cF_and_R = (R ? F_t.and_cardinality(*R) : 0);
  
      // cF_C
      uint64_t cF_C = 0;
      switch (filt) {
        case CiterFilter::OnlyUS:
        case CiterFilter::OnlyCN:
        case CiterFilter::OnlyEU:
          cF_C = cF_and_R;                         // |F_t ∩ R|
          break;
        case CiterFilter::ExcludeUS:
        case CiterFilter::ExcludeCN:
        case CiterFilter::ExcludeEU:
          cF_C = cF - cF_and_R;                    // |F_t| - |F_t ∩ R|
          break;
        default:
          cF_C = cF;                               // no filter
      }
  
          // Precompute common values
    uint64_t cFB_all = F_t.and_cardinality(*B_any);          // |F_t ∩ B_any|
    uint64_t bwin_all = B_any->and_cardinality(*W_t);        // |B_any ∩ W_t|
    
    // Compute filtered cardinalities
    uint64_t cFB_C = 0;
    uint64_t bwin_C = 0;
    
    if (!R) {
        // No filter case
        cFB_C = cFB_all;
        bwin_C = bwin_all;
    } else {
        // Build B_any_R ONCE and reuse for both computations
        // TODO: If this shows up in profiling, cache (focal_id, region) -> B_any_R
        Roaring B_any_R = *B_any & *R;                       // materialize intersection
        uint64_t cFB_R = F_t.and_cardinality(B_any_R);       // |F_t ∩ B_any ∩ R|
        uint64_t bwin_R = B_any_R.and_cardinality(*W_t);     // |B_any ∩ R ∩ W_t|
        
        bool include = (filt == CiterFilter::OnlyUS || 
                       filt == CiterFilter::OnlyCN || 
                       filt == CiterFilter::OnlyEU);
        
        if (include) {
            cFB_C = cFB_R;                                   // include-only
            bwin_C = bwin_R;
        } else {
            cFB_C = cFB_all - cFB_R;                        // exclude
            bwin_C = bwin_all - bwin_R;
        }
    }
  
      // Denominator and return
      uint64_t denom = cF_C + bwin_C - cFB_C;
      if (!denom) return 0.0;
      double num = double(cF_C) - 2.0*double(cFB_C);
      return num / double(denom);
  }
  
size_t Graph::iindex(VertexId focal_id, time_delta_t dt) {
    auto fit = vertices_.find(focal_id);
    if (fit == vertices_.end()) return 0;
    
    Vertex* focal = fit->second;
    timestamp_t ft = focal->time;
    
    // Count papers that cite the focal paper within time window (legacy: no lower bound, ≤ f.time+dt)
    size_t count = 0;
    auto incoming_it = incoming_edges_.find(focal_id);
    if (incoming_it != incoming_edges_.end()) {
        for (auto* citer : incoming_it->second) {
            if (citer->time <= ft + dt) {  // Legacy behavior: no lower bound
                count++;
            }
        }
    }
    return count;
}

double Graph::mcdindex(VertexId focal_id, time_delta_t dt) {
    double cdindex_value = cdindex(focal_id, dt);
    size_t iindex_value = iindex(focal_id, dt);
    return cdindex_value * iindex_value;
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
std::vector<Vertex*> Graph::get_citers(VertexId focal_id, time_delta_t t_delta) {
    // Return all papers that cite the focal paper within the time window
    auto vit = vertices_.find(focal_id);
    if (vit == vertices_.end()) return {};
    Vertex* focal_paper = vit->second;
    timestamp_t focal_time = focal_paper->time;
    std::vector<Vertex*> citers;
    // Incoming edges map holds citers of the focal (source vertices)
    auto in_it = incoming_edges_.find(focal_id);
    if (in_it == incoming_edges_.end()) return citers;
    for (Vertex* citer : in_it->second) {
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

void EnhancedGraph::evict_lru_cache_entry() {
    // Remove the least recently used entry (back of the list)
    if (!cache_lru_order_.empty()) {
        std::string lru_key = cache_lru_order_.back();
        cache_lru_order_.pop_back();
        cache_lru_map_.erase(lru_key);
        filter_bitmap_cache_.erase(lru_key);
    }
}

void EnhancedGraph::update_cache_access(const std::string& key) {
    auto it = cache_lru_map_.find(key);
    if (it != cache_lru_map_.end()) {
        // Move to front (most recently used)
        cache_lru_order_.erase(it->second);
    }
    cache_lru_order_.push_front(key);
    cache_lru_map_[key] = cache_lru_order_.begin();
}

void EnhancedGraph::clear_filter_cache() {
    std::lock_guard<std::mutex> l(filter_mutex_);
    filter_bitmap_cache_.clear();
    cache_lru_order_.clear();
    cache_lru_map_.clear();
}

void EnhancedGraph::clear_predecessor_cache() {
    std::lock_guard<std::mutex> l(predecessor_mutex_);
    predecessor_bitmap_cache_.clear();
    predecessor_bitmap_cache_unfiltered_.clear();
    
    std::lock_guard<std::mutex> lk(bany_mu_);
    bany_lru_.clear();
}

const Roaring& EnhancedGraph::get_cached_predecessor_bitmap_ref(VertexId pred_id, timestamp_t focal_time, time_delta_t dt) {
    std::lock_guard<std::mutex> l(predecessor_mutex_);
    
    // Use composite key including focal_time and dt for correct caching
    PredWinKey key{pred_id, focal_time, dt};
    ++g_benchmark.hf_all;  // Track cache access
    if (auto p = predecessor_bitmap_cache_.get_ptr(key)) {
        ++g_benchmark.hf_hit;  // Track cache hit
        return *p;  // Cache hit - return reference, no copy!
    }
    
    // Cache miss - compute and store
            Roaring result;
    auto edge_it = incoming_edges_.find(pred_id);
    if (edge_it != incoming_edges_.end()) {
        result = incoming_edges_sorted_by_time_ ? 
                 build_ref_bitmap_sorted(edge_it->second, focal_time, dt) :
                 build_ref_bitmap(edge_it->second, focal_time, dt);
    }
    
    // LRU handles eviction automatically, return reference to stored bitmap
    return *predecessor_bitmap_cache_.put_and_get_ptr(key, std::move(result));
}
        
const Roaring& EnhancedGraph::get_cached_predecessor_bitmap_unfiltered_ref(VertexId pred_id) {
    std::lock_guard<std::mutex> l(predecessor_mutex_);
    ++g_benchmark.hu_all;  // Track cache access
    if (auto p = predecessor_bitmap_cache_unfiltered_.get_ptr(pred_id)) {
        ++g_benchmark.hu_hit;  // Track cache hit
        return *p;  // Cache hit - return reference, no copy!
    }
    
    Roaring result;
    auto in_it = incoming_edges_.find(pred_id);
    if (in_it != incoming_edges_.end()) {
        for (Vertex* citer : in_it->second) result.add(citer->id);
    }
    
    // LRU handles eviction automatically, return reference to stored bitmap
    return *predecessor_bitmap_cache_unfiltered_.put_and_get_ptr(pred_id, std::move(result));
}

const Roaring& EnhancedGraph::get_bany_for_focal(VertexId fid) {
    std::lock_guard<std::mutex> lk(bany_mu_);
    ++g_benchmark.hb_all;  // Track cache access
    if (auto p = bany_lru_.get_ptr(fid)) {
        ++g_benchmark.hb_hit;  // Track cache hit
        return *p;  // Cache hit - return reference, no copy!
    }
    
    // Build B_any = union of all unfiltered citers of focal's predecessors
    Roaring r;
    auto focal_it = vertices_.find(fid);
    if (focal_it != vertices_.end()) {
        for (auto* e : focal_it->second->outgoing_edges) {
            auto in_it = incoming_edges_.find(e->target->id);
            if (in_it != incoming_edges_.end())
                for (Vertex* citer : in_it->second) r.add(citer->id);
        }
    }
    
    // Optimize large B_any bitmaps as requested
    r.runOptimize();
    
    // LRU handles eviction automatically, return reference to stored bitmap
    return *bany_lru_.put_and_get_ptr(fid, std::move(r));
}

const Roaring& EnhancedGraph::get_window_bitmap(timestamp_t ft, time_delta_t dt) {
    std::lock_guard<std::mutex> lk(timewin_mu_);
    ++g_benchmark.hb_all;  // Track time window cache access
    
    TimeWinKey key{ft, dt};
    if (auto p = timewin_lru_.get_ptr(key)) {
        ++g_benchmark.hb_hit;  // Track cache hit
        return *p;  // Cache hit - return reference, no copy!
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
    
    Roaring W = properties.get_window_bitmap_by_union("year", start_year, end_year);
    
    // LRU handles eviction automatically, return reference to stored bitmap
    return *timewin_lru_.put_and_get_ptr(key, std::move(W));
}



void EnhancedGraph::build_region_bitmaps_from(PropertyStore& props,
    const CountryLists& lists) {
// lists.us = {"United States"}; lists.cn = {"China", "Hong Kong", ...}
// lists.eu = {"France","Germany",...} from country_list.tsv

auto bm_from = [&](const std::vector<int>& codes) {
return props.get_combined_bitmap("country", codes);
};
regions_.us = bm_from(lists.us_codes);
regions_.cn = bm_from(lists.cn_codes);
regions_.eu = bm_from(lists.eu_codes);
regions_.us.runOptimize(); regions_.cn.runOptimize(); regions_.eu.runOptimize();
}

const Roaring* EnhancedGraph::region_bitmap_for(CiterFilter f) {
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
