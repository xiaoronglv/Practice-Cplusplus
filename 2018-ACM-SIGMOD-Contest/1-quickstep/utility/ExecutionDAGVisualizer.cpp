#include "utility/ExecutionDAGVisualizer.hpp"

#include <cstddef>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "operators/AggregateOperator.hpp"
#include "operators/DropTableOperator.hpp"
#include "operators/PrintOperator.hpp"
#include "operators/SelectOperator.hpp"
#include "optimizer/ExecutionPlan.hpp"
#include "utility/StringUtil.hpp"

namespace project {

ExecutionDAGVisualizer::ExecutionDAGVisualizer(const ExecutionPlan &plan) {
  const std::unordered_set<std::string> no_display_op_types =
      { "DropRelationOperator" };

  num_nodes_ = plan.size();

  // Collect DAG vertices info.
  std::vector<bool> display_ops(num_nodes_, false);
  for (std::size_t node_index = 0; node_index < num_nodes_; ++node_index) {
    const auto &node = plan.getRelationalOperator(node_index);
    const std::string op_name = node.getName();

    if (no_display_op_types.find(op_name) != no_display_op_types.end()) {
      continue;
    }

    display_ops[node_index] = true;
    NodeInfo &node_info = nodes_[node_index];
    node_info.id = node_index;
    node_info.labels.emplace_back(
        "[" + std::to_string(node_index) + "] " + node.getName());

    input_relation_ = nullptr;
    header_info_.clear();

#define GET_NTH(_T, _1, _2, NAME, ...) NAME

#define TRY_FETCH(...) \
    GET_NTH(__VA_ARGS__, TRY_FETCH_2, TRY_FETCH_1)(__VA_ARGS__)

#define TRY_FETCH_1(OpType, info) \
    tryFetchInputRelation<OpType>(node, #OpType, info);

#define TRY_FETCH_2(OpType, info, functor) \
    tryFetchInputRelation<OpType>(node, #OpType, info, functor);

    do {
      TRY_FETCH(BasicAggregateOperator, "input");
      TRY_FETCH(BasicSelectOperator, "input");
      TRY_FETCH(DropTableOperator, "drop",
                [](const auto &op) -> const Relation& { return op.getRelation(); });
      TRY_FETCH(PrintSingleTupleToStringOperator, "input");
    } while (false);

#undef TRY_FETCH_1
#undef TRY_FETCH_2
#undef TRY_FETCH
#undef GET_NTH

    if (input_relation_ != nullptr && !input_relation_->isTemporary()) {
      node_info.labels.emplace_back(
          header_info_ + " relation [" + input_relation_->getName() + "]");
    }
  }

  // Collect DAG edges info.
  for (std::size_t node_index = 0; node_index < num_nodes_; ++node_index) {
    if (display_ops[node_index]) {
      for (const auto &dependent : plan.getDependents(node_index)) {
        if (display_ops[dependent]) {
          edges_.emplace_back();
          edges_.back().src_node_id = node_index;
          edges_.back().dst_node_id = dependent;
        }
      }
    }
  }
}

template <typename OpType>
bool ExecutionDAGVisualizer::tryFetchInputRelation(const RelationalOperator &op,
                                                   const std::string &op_name,
                                                   const std::string &header) {
  if (op.getName() != op_name) {
    return false;
  }
  input_relation_ = &(static_cast<const OpType&>(op).getInputRelation());
  header_info_ = header;
  return true;
}

template <typename OpType, typename Functor>
bool ExecutionDAGVisualizer::tryFetchInputRelation(const RelationalOperator &op,
                                                   const std::string &op_name,
                                                   const std::string &header,
                                                   const Functor &functor) {
  if (op.getName() != op_name) {
    return false;
  }
  input_relation_ = &(functor(static_cast<const OpType&>(op)));
  header_info_ = header;
  return true;
}

std::string ExecutionDAGVisualizer::toDOT() const {
  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=16 fontcolor=gray penwidth=2]\n\n";

  // Format nodes
  for (const auto &node_pair : nodes_) {
    const NodeInfo &node_info = node_pair.second;
    graph_oss << "  " << node_info.id << " [ ";
    if (!node_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(ConcatToString(node_info.labels, "&#10;"))
                << "\" ";
    }
    if (!node_info.color.empty()) {
      graph_oss << "style=filled fillcolor=\"" << node_info.color << "\" ";
    }
    graph_oss << "]\n";
  }
  graph_oss << "\n";

  // Format edges
  for (const EdgeInfo &edge_info : edges_) {
    graph_oss << "  " << edge_info.src_node_id << " -> "
              << edge_info.dst_node_id << " [ ";
    if (!edge_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(ConcatToString(edge_info.labels, "&#10;"))
                << "\" ";
    }
    graph_oss << "]\n";
  }
  graph_oss << "}\n";

  return graph_oss.str();
}

}  // namespace project
