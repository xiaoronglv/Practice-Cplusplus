#ifndef PROJECT_UTILITY_EXECUTION_DAG_VISUALIZER_HPP_
#define PROJECT_UTILITY_EXECUTION_DAG_VISUALIZER_HPP_

#include <cstddef>
#include <map>
#include <string>
#include <vector>

#include "optimizer/ExecutionPlan.hpp"
#include "utility/Macros.hpp"

namespace project {

class Relation;
class RelationalOperator;

class ExecutionDAGVisualizer {
 public:
  explicit ExecutionDAGVisualizer(const ExecutionPlan &plan);

  std::string toDOT() const;

 private:
  struct NodeInfo {
    std::size_t id;
    std::vector<std::string> labels;
    std::string color;
  };

  struct EdgeInfo {
    std::size_t src_node_id;
    std::size_t dst_node_id;
    std::vector<std::string> labels;
  };

  template <typename OpType>
  bool tryFetchInputRelation(const RelationalOperator &op,
                             const std::string &op_name,
                             const std::string &header);

  template <typename OpType, typename Functor>
  bool tryFetchInputRelation(const RelationalOperator &op,
                             const std::string &op_name,
                             const std::string &header,
                             const Functor &functor);

  std::size_t num_nodes_;
  std::map<std::size_t, NodeInfo> nodes_;
  std::vector<EdgeInfo> edges_;

  const Relation *input_relation_;
  std::string header_info_;

  DISALLOW_COPY_AND_ASSIGN(ExecutionDAGVisualizer);
};

}  // namespace project

#endif  // PROJECT_UTILITY_EXECUTION_DAG_VISUALIZER_HPP_
