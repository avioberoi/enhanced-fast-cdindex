#pragma once

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <deque>
#include <mutex>
#include <list>
#include <arrow/api.h>
#include <roaring/roaring.hh>
#include <absl/container/flat_hash_map.h>
#include <memory>
#include <type_traits>
#include <chrono>
#include <string>

using VertexId = uint32_t;
using timestamp_t = int64_t;
using time_delta_t = int64_t;

// Forward declarations
struct FilterExpression;

// Composite key for predecessor bitmap cache that includes time window
struct PredWinKey {
    VertexId pred;
    timestamp_t focal_time;
    time_delta_t dt;
    bool operator==(const PredWinKey& o) const {
        return pred == o.pred && focal_time == o.focal_time && dt == o.dt;
    }
};

struct PredWinKeyHash {
    size_t operator()(const PredWinKey& k) const {
        // 64-bit mix; stronger combiner for hash collision resistance
        uint64_t x = (uint64_t)k.pred ^ (uint64_t)k.focal_time * 0x9e3779b97f4a7c15ULL;
        x ^= (uint64_t)k.dt + 0x9e3779b97f4a7c15ULL + (x<<6) + (x>>2);
        return (size_t)x;
    }
};

// Time window key for caching (focal_time, dt) window bitmaps
struct TimeWinKey {
    timestamp_t ft;
    time_delta_t dt;
    bool operator==(const TimeWinKey& o) const { 
        return ft == o.ft && dt == o.dt; 
    }
};

struct TimeWinKeyHash {
    size_t operator()(const TimeWinKey& k) const {
        uint64_t x = (uint64_t)k.ft * 0x9e3779b97f4a7c15ULL;
        x ^= (uint64_t)k.dt + 0x9e3779b97f4a7c15ULL + (x<<6) + (x>>2);
        return (size_t)x;
    }
};

// Tiny LRU cache template for efficient eviction with shared_ptr support
template<typename K, typename V, typename Hash = std::hash<K>>
class TinyLRU {
    using It = typename std::list<K>::iterator;
    size_t cap_;
    std::list<K> order_; // front = MRU
    std::unordered_map<K, std::pair<V, It>, Hash> map_;
public:
    explicit TinyLRU(size_t cap): cap_(cap) {}
    bool get(const K& k, V& out) {
        auto it = map_.find(k);
        if (it == map_.end()) return false;
        order_.splice(order_.begin(), order_, it->second.second);
        out = it->second.first;
        return true;
    }
    const V* get_ptr(const K& k) {
        auto it = map_.find(k);
        if (it == map_.end()) return nullptr;
        order_.splice(order_.begin(), order_, it->second.second);
        return &it->second.first;
    }
    // // New method for shared_ptr types - returns copy of shared_ptr for safe lifetime
    // template<typename T>
    // std::shared_ptr<T> get_shared(const K& k, std::enable_if_t<std::is_same_v<V, std::shared_ptr<T>>, int> = 0) {
    //     auto it = map_.find(k);
    //     if (it == map_.end()) return nullptr;
    //     order_.splice(order_.begin(), order_, it->second.second);
    //     return it->second.first;  // Returns copy of shared_ptr, incrementing refcount
    // }
    void put(const K& k, V v) {
        auto it = map_.find(k);
        if (it != map_.end()) {
            it->second.first = std::move(v);
            order_.splice(order_.begin(), order_, it->second.second);
            return;
        }
        if (map_.size() == cap_) {
            K ev = order_.back(); order_.pop_back(); map_.erase(ev);
        }
        order_.push_front(k);
        map_.emplace(k, std::make_pair(std::move(v), order_.begin()));
    }
    // // New method for shared_ptr types - returns copy for safe lifetime
    // template<typename T>
    // std::shared_ptr<T> put_and_get_shared(const K& k, V v, std::enable_if_t<std::is_same_v<V, std::shared_ptr<T>>, int> = 0) {
    //     auto it = map_.find(k);
    //     if (it != map_.end()) {
    //         it->second.first = std::move(v);
    //         order_.splice(order_.begin(), order_, it->second.second);
    //         return it->second.first;
    //     }
    //     if (map_.size() == cap_) {
    //         K ev = order_.back(); order_.pop_back(); map_.erase(ev);
    //     }
    //     order_.push_front(k);
    //     auto [pos, _] = map_.emplace(k, std::make_pair(std::move(v), order_.begin()));
    //     return pos->second.first;
    // }
    // Generic helpers that return a copy of the stored value.
    // For shared_ptr V, this safely bumps refcount and returns a usable handle.
    V get_copy(const K& k) {
        auto it = map_.find(k);
        if (it == map_.end()) return {};
        order_.splice(order_.begin(), order_, it->second.second);
        return it->second.first; // copy shared_ptr (bump refcount) or copy value
    }
    V put_and_get_copy(const K& k, V v) {
        auto it = map_.find(k);
        if (it != map_.end()) {
            it->second.first = std::move(v);
            order_.splice(order_.begin(), order_, it->second.second);
            return it->second.first;
        }
        if (map_.size() == cap_) {
            K ev = order_.back(); order_.pop_back(); map_.erase(ev);
        }
        order_.push_front(k);
        auto [pos, _] = map_.emplace(k, std::make_pair(std::move(v), order_.begin()));
        return pos->second.first;
    }
    V* put_and_get_ptr(const K& k, V v) {
        auto it = map_.find(k);
        if (it != map_.end()) {
            it->second.first = std::move(v);
            order_.splice(order_.begin(), order_, it->second.second);
            return &it->second.first;
        }
        if (map_.size() == cap_) {
            K ev = order_.back(); order_.pop_back(); map_.erase(ev);
        }
        order_.push_front(k);
        auto [pos, _] = map_.emplace(k, std::make_pair(std::move(v), order_.begin()));
        return &pos->second.first;
    }
    void clear() { map_.clear(); order_.clear(); }
    size_t size() const { return map_.size(); }
};

// runtime‐tunable thresholds
int64_t BATCH_PARALLEL_THRESHOLD();
int64_t INNER_PARALLEL_THRESHOLD();
int64_t MAX_CACHE_ENTRIES();
int64_t CHUNK_SIZE();

// Micro-benchmarking infrastructure
struct CDIndexBenchmark {
    double t1 = 0.0;  // F_t build time (ms)
    double t2 = 0.0;  // B_t build time (ms)
    double t3 = 0.0;  // Cardinality ops time (ms)
    uint64_t n = 0;   // Number of CD computations
    
    // Cache hit rates
    uint64_t hf_hit = 0, hf_all = 0;  // Predecessor cache (filtered)
    uint64_t hb_hit = 0, hb_all = 0;  // B_any cache
    uint64_t hu_hit = 0, hu_all = 0;  // Predecessor cache (unfiltered)
    
    void reset() {
        t1 = t2 = t3 = 0.0;
        n = hf_hit = hf_all = hb_hit = hb_all = hu_hit = hu_all = 0;
    }
    
    void print_summary() const;
};

extern CDIndexBenchmark g_benchmark;

// Scoped timer for automatic time accumulation
struct ScopedTimer {
    double& accumulator;
    std::chrono::high_resolution_clock::time_point start;
    
    explicit ScopedTimer(double& acc) : accumulator(acc), start(std::chrono::high_resolution_clock::now()) {}
    ~ScopedTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        accumulator += std::chrono::duration<double, std::milli>(end - start).count();
    }
};

struct Edge;
class Graph;
class PropertyStore;
class EnhancedGraph;

// Forward declarations for filtering
enum class CiterFilter {
    None,
    ExcludeUS, ExcludeCN, ExcludeEU,
    OnlyUS,    OnlyCN,    OnlyEU
};

struct RegionSets {
    Roaring us, cn, eu;
};

struct Vertex {
  VertexId id;
  timestamp_t time;
  std::vector<Edge*> outgoing_edges;
  Vertex(VertexId id, timestamp_t time);
  void shrink_to_fit();
  void sort_outgoing_edges();
  bool has_outgoing_to(VertexId target_id) const;
};

struct Edge {
  Vertex* source;
  Vertex* target;
  Edge(Vertex* s, Vertex* t);
};

class PropertyStore {
public:
  arrow::Status ingest_arrow(const std::shared_ptr<arrow::Table>& table);
  void build_indexes();
  void add_categorical(VertexId id, const std::string& name, int value);
  Roaring get_combined_bitmap(const std::string& prop_name,
                              const std::vector<int>& values) const;
  // String → code helpers (use the internal dictionary)
  std::vector<int> get_codes_for_strings(const std::string& prop,
                                         const std::vector<std::string>& names) const;
  Roaring get_combined_bitmap_str(const std::string& prop_name,
                                  const std::vector<std::string>& names) const;
  // Build W_t by unioning year bitmaps (no prefix storage needed)
  Roaring get_window_bitmap_by_union(const std::string& prop_name, int start_year, int end_year) const;
  arrow::Status ingest_country_direct(const std::shared_ptr<arrow::Table>& table,
                                      const std::unordered_map<std::string, VertexId>& uid2id,
                                      const std::string& uid_col = "UID",
                                      const std::string& country_col = "country");
  void clear();
private:
    // DISABLED: void build_prefix_or_arrays();  // Causes memory explosion - removed
    
    absl::flat_hash_map<std::string, absl::flat_hash_map<int, Roaring>> categorical_bitmaps_;
    
    // REMOVED: prefix_or_bitmaps_ - was causing 119GB memory usage
    
    std::unordered_map<std::string, std::vector<std::pair<VertexId,int>>> categorical_properties_;
    std::unordered_map<std::string, std::unordered_map<std::string,int>> string_dictionaries_;
};

class Graph {
public:
  virtual ~Graph();
  void add_vertex(VertexId id, timestamp_t time);
  void add_edge(VertexId s, VertexId t);
  double cdindex(VertexId focal_id, time_delta_t dt);
  double cdindex_core(VertexId focal_id, time_delta_t dt,
                      const Roaring* region, bool include_region);
  void prepare_for_searching();
  void sort_incoming_edges_by_time();
  
  // CD index variants and metrics
  size_t iindex(VertexId focal_id, time_delta_t dt);
  double mcdindex(VertexId focal_id, time_delta_t dt);
  double cdindex_filtered(VertexId focal_id, time_delta_t dt, CiterFilter filter);
  
  // Degree functions
  size_t in_degree(VertexId id) const;
  size_t out_degree(VertexId id) const;
  std::vector<VertexId> in_edges(VertexId id) const;
  std::vector<VertexId> out_edges(VertexId id) const;
  timestamp_t get_timestamp(VertexId id) const;
  
  // Public getters for metrics
  [[nodiscard]] size_t vertex_count() const noexcept { return vertices_.size(); }
  [[nodiscard]] size_t edge_count() const noexcept { return all_edges_.size(); }
protected:
  std::vector<Vertex*> get_citers(VertexId focal_id, time_delta_t dt);
  absl::flat_hash_map<VertexId, Vertex*> vertices_;
  absl::flat_hash_map<VertexId, std::vector<Vertex*>> incoming_edges_;
  std::vector<Edge*> all_edges_;
  bool incoming_edges_sorted_by_time_ = false;
};

class EnhancedGraph : public Graph {
public:
  PropertyStore properties;
  
  // Constructor to initialize LRU caches with increased sizes to reduce thrash
  EnhancedGraph() : predecessor_bitmap_cache_(std::max(int64_t(512), MAX_CACHE_ENTRIES())), 
                    predecessor_bitmap_cache_unfiltered_(std::max(int64_t(512), MAX_CACHE_ENTRIES())),
                    bany_lru_(std::max(int64_t(512), MAX_CACHE_ENTRIES())),
                    timewin_lru_(1024) {}
  void add_vertices_from_arrow(const std::shared_ptr<arrow::Table>& table);
  void add_edges_from_arrow(const std::shared_ptr<arrow::Table>& table);
  void clear_filter_cache();
  void clear_predecessor_cache();
  
  // SAFE CACHE ACCESS: Return shared_ptr to prevent use-after-free
  std::shared_ptr<const Roaring> get_cached_predecessor_bitmap_sp(VertexId pred_id, timestamp_t focal_time, time_delta_t dt);
  std::shared_ptr<const Roaring> get_cached_predecessor_bitmap_unfiltered_sp(VertexId pred_id);
  std::shared_ptr<const Roaring> get_bany_for_focal_sp(VertexId fid) const;
  
  struct CountryLists {
    std::vector<std::string> us_names, cn_names, eu_names; // normalized lowercase
  };
  void set_country_lists(CountryLists lists);
  void build_region_bitmaps_from(PropertyStore& props, const CountryLists& lists);
  
  // Window bitmap optimization - safe shared_ptr access
  std::shared_ptr<const Roaring> get_window_bitmap_sp(timestamp_t ft, time_delta_t dt) const;
  
  // Returns nullptr if filter == None. Thread-safe; lazily builds region bitmaps on first call
  [[nodiscard]] const Roaring* region_bitmap_for(CiterFilter filter) const;

  // Ingest country parquet directly (uses internal UID→id map)
  arrow::Status ingest_countries_from_parquet(const std::shared_ptr<arrow::Table>& table,
                                              const std::string& uid_col = "UID",
                                              const std::string& country_col = "country");

private:
  // Predecessor bitmap caching with safe shared ownership
  mutable std::mutex predecessor_mutex_;
  mutable TinyLRU<PredWinKey, std::shared_ptr<const Roaring>, PredWinKeyHash> predecessor_bitmap_cache_;  // (pred_id, focal_time, dt) -> bitmap of papers citing pred in window
  mutable TinyLRU<VertexId, std::shared_ptr<const Roaring>> predecessor_bitmap_cache_unfiltered_; // unfiltered citers of pred
  
  // B_any per-focal caching (time-invariant) with safe shared ownership
  mutable TinyLRU<VertexId, std::shared_ptr<const Roaring>> bany_lru_;
  mutable std::mutex bany_mu_;
  
  // Time window bitmap caching with safe shared ownership
  mutable TinyLRU<TimeWinKey, std::shared_ptr<const Roaring>, TimeWinKeyHash> timewin_lru_;
  mutable std::mutex timewin_mu_;
  
  // Region filtering support (from user's additions)
  mutable RegionSets regions_;
  mutable std::once_flag regions_once_;
  CountryLists country_lists_;  // set via set_country_lists()
  // Internal UID→id map (built when vertices provide UID)
  std::unordered_map<std::string, VertexId> uid2id_;

//   std::mutex filter_mutex_;
//   std::unordered_map<std::string, Roaring> filter_bitmap_cache_;
//   std::list<std::string> cache_lru_order_;  // LRU order: most recent at front
//   std::unordered_map<std::string, std::list<std::string>::iterator> cache_lru_map_;  // Fast lookup of iterators
  
//   void evict_lru_cache_entry();
//   void update_cache_access(const std::string& key);
};