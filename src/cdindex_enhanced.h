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

using VertexId = uint32_t;
using timestamp_t = int64_t;
using time_delta_t = int64_t;

// Forward declarations
struct FilterExpression;

// runtime‐tunable thresholds
int64_t BATCH_PARALLEL_THRESHOLD();
int64_t INNER_PARALLEL_THRESHOLD();
int64_t MAX_CACHE_ENTRIES();
int64_t CHUNK_SIZE();

struct Edge;
class Graph;
class PropertyStore;
class EnhancedGraph;

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
  Roaring evaluate_filter_expression(const FilterExpression& expr) const;
  void clear();
private:
    absl::flat_hash_map<std::string, absl::flat_hash_map<int, Roaring>> categorical_bitmaps_;
    std::unordered_map<std::string, std::vector<std::pair<VertexId,int>>> categorical_properties_;
    std::unordered_map<std::string, std::unordered_map<std::string,int>> string_dictionaries_;
};

class Graph {
public:
  virtual ~Graph();
  void add_vertex(VertexId id, timestamp_t time);
  void add_edge(VertexId s, VertexId t);
  double cdindex(VertexId focal_id, time_delta_t dt);
  double compute_cdindex_logic(VertexId focal_id,
                               const std::vector<Vertex*>& citers,
                               time_delta_t dt);
  void prepare_for_searching();
  
  // Public getters for metrics
  size_t vertex_count() const { return vertices_.size(); }
  size_t edge_count() const { return all_edges_.size(); }
protected:
  std::vector<Vertex*> get_citers(VertexId focal_id, time_delta_t dt);
  absl::flat_hash_map<VertexId, Vertex*> vertices_;
  absl::flat_hash_map<VertexId, std::vector<Vertex*>> incoming_edges_;
  std::vector<Edge*> all_edges_;
};

class EnhancedGraph : public Graph {
public:
  PropertyStore properties;
  void add_vertices_from_arrow(const std::shared_ptr<arrow::Table>& table);
  void add_edges_from_arrow(const std::shared_ptr<arrow::Table>& table);
  double cdindex_filtered(VertexId focal_id, time_delta_t t_delta, const std::unordered_map<std::string,std::vector<int>>& filters);
  std::shared_ptr<arrow::Table> cdindex_batch(
      const std::shared_ptr<arrow::UInt32Array>& pids, time_delta_t dt);
  std::shared_ptr<arrow::Table> cdindex_filtered_batch(
      const std::shared_ptr<arrow::UInt32Array>& pids, time_delta_t dt,
      const std::unordered_map<std::string, std::vector<int>>& filters);
  void clear_filter_cache();

private:
  std::mutex filter_mutex_;
  std::unordered_map<std::string, Roaring> filter_bitmap_cache_;
  std::list<std::string> cache_lru_order_;  // LRU order: most recent at front
  std::unordered_map<std::string, std::list<std::string>::iterator> cache_lru_map_;  // Fast lookup of iterators
  
  void evict_lru_cache_entry();
  void update_cache_access(const std::string& key);
};

// Enhanced filter system supporting AND, OR, NOT operations
enum class FilterOp { AND, OR, NOT, EQUALS, IN };

struct FilterExpression {
    FilterOp op;
    std::string attribute;
    std::vector<int> values;
    std::vector<std::unique_ptr<FilterExpression>> children;
    
    FilterExpression(FilterOp operation) : op(operation) {}
    FilterExpression(FilterOp operation, const std::string& attr, const std::vector<int>& vals) 
        : op(operation), attribute(attr), values(vals) {}
};

// Helper functions for building filter expressions
std::unique_ptr<FilterExpression> make_filter_and(std::vector<std::unique_ptr<FilterExpression>> children);
std::unique_ptr<FilterExpression> make_filter_or(std::vector<std::unique_ptr<FilterExpression>> children);
std::unique_ptr<FilterExpression> make_filter_not(std::unique_ptr<FilterExpression> child);
std::unique_ptr<FilterExpression> make_filter_in(const std::string& attribute, const std::vector<int>& values);
std::unique_ptr<FilterExpression> make_filter_equals(const std::string& attribute, int value);
