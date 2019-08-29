#include "utility/PlanVisualizer.hpp"

#include <cstddef>
#include <exception>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/ChainedJoinAggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/MultiwayEquiJoinAggregate.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "storage/Relation.hpp"
#include "utility/Range.hpp"
#include "utility/StringUtil.hpp"

#include "glog/logging.h"

namespace project {

using namespace optimizer;  // NOLINT[build/namespaces]

std::string PlanVisualizer::visualize(const PlanPtr &input) {
  DCHECK(input->getPlanType() == PlanType::kTopLevelPlan);

  color_map_["Aggregate"] = "#decbe4";
  color_map_["BuildSideFKIndexJoinAggregate"] = "#e5d8bd";
  color_map_["ChainedJoinAggregate"] = "#ffa07a";
  color_map_["FKIndexPKIndexJoinAggregate"] = "#e5d8bd";
  color_map_["FKPKIndexJoinAggregate"] = "#e5d8bd";
  color_map_["FKPKScanJoinAggregate"] = "#e5d8bd";
  color_map_["FilterJoin"] = "#fddaec";
  color_map_["HashJoin"] = "#fbb4ae";
  color_map_["HashJoinAggregate"] = "#ffa07a";
  color_map_["HashLeftSemiJoin"] = "orange";
  color_map_["IndexScan"] = "#fed9a6";
  color_map_["MultiwayEquiJoinAggregate"] = "#ffa07a";
  color_map_["PKIndexJoin"] = "#fed9a6";
  color_map_["Selection"] = "#ccebc5";
  color_map_["SortMergeJoin"] = "#fbb4ae";
  color_map_["TableReference"] = "#c3ddf3";
  color_map_["TableView"] = "#c3ddf3";

  visit(input);

  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=16 fontcolor=gray30 penwidth=2]\n\n";

  // Format nodes
  for (const NodeInfo &node_info : nodes_) {
    graph_oss << "  " << node_info.id << " [";
    if (!node_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(ConcatToString(node_info.labels, "&#10;"))
                << "\"";
    }
    if (!node_info.color.empty()) {
      graph_oss << " style=filled fillcolor=\"" << node_info.color << "\"";
    }
    graph_oss << "]\n";
  }
  graph_oss << "\n";

  // Format edges
  for (const EdgeInfo &edge_info : edges_) {
    graph_oss << "  " << edge_info.src_node_id << " -> "
              << edge_info.dst_node_id << " [";
    if (edge_info.dashed) {
      graph_oss << "style=dashed ";
    }
    if (!edge_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(ConcatToString(edge_info.labels, "&#10;"))
                << "\"";
    }
    graph_oss << "]\n";
  }

  graph_oss << "}\n";

  return graph_oss.str();
}

void PlanVisualizer::visit(const PlanPtr &input) {
  int node_id = ++id_counter_;
  node_id_map_.emplace(input, node_id);

  std::set<ExprId> referenced_ids;
  for (const auto &attr : input->getReferencedAttributes()) {
    referenced_ids.emplace(attr->id());
  }
  for (const auto &child : input->children()) {
    visit(child);

    int child_id = node_id_map_[child];

    edges_.emplace_back(EdgeInfo());
    EdgeInfo &edge_info = edges_.back();
    edge_info.src_node_id = child_id;
    edge_info.dst_node_id = node_id;
    edge_info.dashed = false;

    if (input->getPlanType() == PlanType::kEquiJoin &&
        child == input->children()[1]) {
      edge_info.dashed = true;
    }

    const std::vector<ScalarPtr> *output_expressions = nullptr;
    switch (child->getPlanType()) {
      case PlanType::kAggregate:
        output_expressions =
            &std::static_pointer_cast<const Aggregate>(child)->aggregate_expressions();
        break;
      case PlanType::kChainedJoinAggregate:
        output_expressions =
            &std::static_pointer_cast<const ChainedJoinAggregate>(child)->aggregate_expressions();
        break;
      case PlanType::kEquiJoinAggregate:
        output_expressions =
            &std::static_pointer_cast<const EquiJoinAggregate>(child)->aggregate_expressions();
        break;
      case PlanType::kMultiwayEquiJoinAggregate:
        output_expressions =
            &std::static_pointer_cast<const MultiwayEquiJoinAggregate>(child)->aggregate_expressions();
        break;
      case PlanType::kSelection: {
        const SelectionPtr &selection = std::static_pointer_cast<const Selection>(child);
        if (selection->selection_type() == Selection::kBasic) {
          output_expressions = &selection->project_expressions();
        }
        break;
      }
      default:
        break;
    }

    if (output_expressions != nullptr) {
      for (const auto &expr : *output_expressions) {
        if (referenced_ids.find(expr->getAttribute()->id()) != referenced_ids.end()) {
          edge_info.labels.emplace_back(expr->toShortString());
        }
      }
    } else {
      for (const auto &attr : child->getOutputAttributes()) {
        if (referenced_ids.find(attr->id()) != referenced_ids.end()) {
          std::string name("[" + std::to_string(attr->id()) + "] " + attr->name());
          if (cost_model_.impliesUniqueAttributes(child, {attr})) {
            name.append(" *");
          }
          edge_info.labels.emplace_back(name);
        }
      }
    }
  }

  nodes_.emplace_back(NodeInfo());
  NodeInfo &node_info = nodes_.back();
  node_info.id = node_id;
  if (color_map_.find(input->getName()) != color_map_.end()) {
    node_info.color = color_map_[input->getName()];
  }

  switch (input->getPlanType()) {
    case PlanType::kAggregate: {
      const AggregatePtr &aggregate =
          std::static_pointer_cast<const Aggregate>(input);
      node_info.labels.emplace_back(input->getName());
      if (aggregate->filter_predicate() != nullptr) {
        node_info.labels.emplace_back(aggregate->filter_predicate()->toShortString());
      }
      break;
    }
    case PlanType::kChainedJoinAggregate: {
      const ChainedJoinAggregatePtr &chained_join_aggregate =
          std::static_pointer_cast<const ChainedJoinAggregate>(input);
      node_info.labels.emplace_back(input->getName());

      for (const auto &it : chained_join_aggregate->join_attribute_pairs()) {
        node_info.labels.emplace_back(
            "[" + std::to_string(it.first->id()) + "] " + it.first->name() +
            " = " +
            "[" + std::to_string(it.second->id()) + "] " + it.second->name());
      }

      node_info.labels.emplace_back("----------");
      const auto ranges =
          cost_model_.inferJoinAttributeRanges(chained_join_aggregate);
      for (const auto &range : ranges) {
        node_info.labels.emplace_back(
            "[" + std::to_string(range.begin()) +
            ", " + std::to_string(range.end()) + ")");
      }
      node_info.labels.emplace_back("----------");

      for (const auto &predicate : chained_join_aggregate->filter_predicates()) {
        if (predicate) {
          node_info.labels.emplace_back(predicate->toShortString());
        }
      }
      break;
    }
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(input);
      node_info.labels.emplace_back(input->getName());

      const auto &probe_attributes = equi_join->probe_attributes();
      const auto &build_attributes = equi_join->build_attributes();
      for (std::size_t i = 0; i < probe_attributes.size(); ++i) {
        node_info.labels.emplace_back(
            probe_attributes[i]->name() + " = " + build_attributes[i]->name());
      }

      if (equi_join->probe_filter_predicate() != nullptr) {
        node_info.labels.emplace_back(
            equi_join->probe_filter_predicate()->toShortString());
      }
      if (equi_join->build_filter_predicate() != nullptr) {
        node_info.labels.emplace_back(
            equi_join->build_filter_predicate()->toShortString());
      }
      break;
    }
    case PlanType::kEquiJoinAggregate: {
      const EquiJoinAggregatePtr &equi_join_aggr =
          std::static_pointer_cast<const EquiJoinAggregate>(input);
      node_info.labels.emplace_back(input->getName());

      const auto &probe_attributes = equi_join_aggr->probe_attributes();
      const auto &build_attributes = equi_join_aggr->build_attributes();
      for (std::size_t i = 0; i < probe_attributes.size(); ++i) {
        node_info.labels.emplace_back(
            probe_attributes[i]->name() + " = " + build_attributes[i]->name());
      }

      if (equi_join_aggr->probe_filter_predicate() != nullptr) {
        node_info.labels.emplace_back(
            equi_join_aggr->probe_filter_predicate()->toShortString());
      }
      if (equi_join_aggr->build_filter_predicate() != nullptr) {
        node_info.labels.emplace_back(
            equi_join_aggr->build_filter_predicate()->toShortString());
      }
      break;
    }
    case PlanType::kMultiwayEquiJoinAggregate: {
      const MultiwayEquiJoinAggregatePtr &equi_join_aggr =
          std::static_pointer_cast<const MultiwayEquiJoinAggregate>(input);
      node_info.labels.emplace_back(input->getName());
      node_info.labels.emplace_back("----------");

      std::vector<std::string> join_attribute_names;
      for (std::size_t i = 0; i < equi_join_aggr->join_attributes().size(); ++i) {
        const auto &attr = equi_join_aggr->join_attributes().at(i);
        join_attribute_names.emplace_back(attr->name());
      }
      node_info.labels.emplace_back(ConcatToString(join_attribute_names, " = "));

      node_info.labels.emplace_back("----------");
      const Range range =
          cost_model_.inferRange(equi_join_aggr->join_attributes().front()->id(),
                                 equi_join_aggr);
      node_info.labels.emplace_back(
          "range: [" + std::to_string(range.begin()) +
          ", " + std::to_string(range.end()) + ")");
      node_info.labels.emplace_back("----------");

      for (const auto &predicate : equi_join_aggr->filter_predicates()) {
        if (predicate != nullptr) {
          node_info.labels.emplace_back(predicate->toShortString());
        }
      }
      break;
    }
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(input);
      node_info.labels.emplace_back(input->getName());
      if (selection->filter_predicate() != nullptr) {
        node_info.labels.emplace_back(
            selection->filter_predicate()->toShortString());
      }
      break;
    }
    case PlanType::kTableReference: {
      const TableReferencePtr &table_reference =
          std::static_pointer_cast<const TableReference>(input);
      node_info.labels.emplace_back(table_reference->relation().getName());
      break;
    }
    case PlanType::kTableView: {
      const TableViewPtr &table_view =
          std::static_pointer_cast<const TableView>(input);
      node_info.labels.emplace_back(input->getName());
      DCHECK(table_view->comparison() != nullptr);
      node_info.labels.emplace_back(table_view->comparison()->toShortString());
      break;
    }
    default: {
      node_info.labels.emplace_back(input->getName());
      break;
    }
  }

  const std::size_t estimated_cardinality = cost_model_.estimateCardinality(input);
  const double estimated_selectivity = cost_model_.estimateSelectivity(input);

  node_info.labels.emplace_back(
      "est. Cardinality = " + std::to_string(estimated_cardinality));
  node_info.labels.emplace_back(
      "est. Selectivity = " + std::to_string(estimated_selectivity));
}

}  // namespace project
