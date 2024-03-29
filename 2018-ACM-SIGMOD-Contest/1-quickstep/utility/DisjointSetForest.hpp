#ifndef PROJECT_UTILITY_DISJOINT_SET_FOREST_HPP_
#define PROJECT_UTILITY_DISJOINT_SET_FOREST_HPP_

#include <algorithm>
#include <cstddef>
#include <unordered_map>
#include <vector>

#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

template <typename ElementT,
          class MapperT = std::unordered_map<ElementT, std::size_t>>
class DisjointSetForest {
 public:
  DisjointSetForest() {}

  bool hasElement(const ElementT &element) const {
    return elements_map_.find(element) != elements_map_.end();
  }

  void makeSet(const ElementT &element) {
    if (!hasElement(element)) {
      std::size_t pos = nodes_.size();
      nodes_.emplace_back(0, pos);
      elements_map_.emplace(element, pos);
    }
  }

  std::size_t find(const ElementT &element) {
    DCHECK(hasElement(element));

    const std::size_t node_id = elements_map_.at(element);
    std::size_t root_id = node_id;
    std::size_t parent_id;
    while ((parent_id = nodes_[root_id].parent) != root_id) {
      root_id = parent_id;
    }
    compress_path(node_id, root_id);
    return root_id;
  }

  void merge(const ElementT &element1, const ElementT &element2) {
    std::size_t root_id1 = find(element1);
    std::size_t root_id2 = find(element2);
    if (root_id1 != root_id2) {
      Node &n1 = nodes_[root_id1];
      Node &n2 = nodes_[root_id2];
      if (n1.rank > n2.rank) {
        n2.parent = root_id1;
      } else if (n1.rank < n2.rank) {
        n1.parent = root_id2;
      } else {
        n1.parent = root_id2;
        n2.rank += 1;
      }
    }
  }

  bool isConnected(const ElementT &element1, const ElementT &element2) {
    return find(element1) == find(element2);
  }

 private:
  struct Node {
    Node(const std::size_t rank_in, const std::size_t parent_in)
        : rank(rank_in), parent(parent_in) {
    }
    std::size_t rank;
    std::size_t parent;
  };

  inline void compress_path(const std::size_t leaf_node_id,
                            const std::size_t root_node_id) {
    std::size_t node_id = leaf_node_id;
    std::size_t max_rank = 0;
    while (node_id != root_node_id) {
      const Node &node = nodes_[node_id];
      max_rank = std::max(max_rank, node.rank);

      const std::size_t parent_id = node.parent;
      nodes_[node_id].parent = root_node_id;
      node_id = parent_id;
    }
    nodes_[root_node_id].rank = max_rank + 1;
  }

  std::vector<Node> nodes_;
  MapperT elements_map_;

  DISALLOW_COPY_AND_ASSIGN(DisjointSetForest);
};

}  // namespace project

#endif  // PROJECT_UTILITY_DISJOINT_SET_FOREST_HPP_
