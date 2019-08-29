#include "optimizer/rules/SpecializeMultiwayJoinAggregate.hpp"

#include <cstddef>
#include <limits>
#include <memory>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AggregateFunction.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/MultiwayEquiJoinAggregate.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "storage/Attribute.hpp"
#include "storage/Relation.hpp"
#include "utility/ContainerUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr SpecializeMultiwayJoinAggregate::applyToNode(const PlanPtr &node) {
  switch (node->getPlanType()) {
    case PlanType::kAggregate:  // Fall through
    case PlanType::kEquiJoinAggregate:
      break;
    default:
      return node;
  }

  JoinInfo info;
  collect(node, &info);

  if (!info.has_basic_join || info.plans.size() < 2) {
    return node;
  }

  const MultiwayEquiJoinAggregatePtr output =
      MultiwayEquiJoinAggregate::Create(info.plans,
                                        info.join_attributes,
                                        *info.aggregate_expressions,
                                        info.filter_predicates);
  return rewriteAggregateExpressions(output);
}

void SpecializeMultiwayJoinAggregate::collect(
    const PlanPtr &plan, JoinInfo *info) {
  switch (plan->getPlanType()) {
    case PlanType::kAggregate:
      collectAggregate(
          std::static_pointer_cast<const Aggregate>(plan), info);
      break;
    case PlanType::kEquiJoinAggregate:
      collectEquiJoinAggregate(
          std::static_pointer_cast<const EquiJoinAggregate>(plan), info);
      break;
    default:
      LOG(FATAL) << "Unsupported plan type in "
                 << "SpecializeMultiwayJoinAggregate::collect";
  }
}

void SpecializeMultiwayJoinAggregate::collectAggregate(
    const AggregatePtr &aggregate, JoinInfo *info) {
  EquiJoinPtr equi_join;
  if (!SomeEquiJoin::MatchesWithConditionalCast(aggregate->input(), &equi_join)) {
    return;
  }

  info->aggregate_expressions = &aggregate->aggregate_expressions();
  collectEquiJoin(equi_join->probe(),
                  equi_join->build(),
                  equi_join->probe_filter_predicate(),
                  equi_join->build_filter_predicate(),
                  equi_join->probe_attributes(),
                  equi_join->build_attributes(),
                  equi_join->join_type() == EquiJoin::kHashInnerJoin,
                  info);
}

void SpecializeMultiwayJoinAggregate::collectEquiJoinAggregate(
    const EquiJoinAggregatePtr &equi_join_aggregate, JoinInfo *info) {
  info->aggregate_expressions = &equi_join_aggregate->aggregate_expressions();
  collectEquiJoin(equi_join_aggregate->probe(),
                  equi_join_aggregate->build(),
                  equi_join_aggregate->probe_filter_predicate(),
                  equi_join_aggregate->build_filter_predicate(),
                  equi_join_aggregate->probe_attributes(),
                  equi_join_aggregate->build_attributes(),
                  false /* is_basic_join */,
                  info);
}

bool SpecializeMultiwayJoinAggregate::collectEquiJoin(
    PlanPtr probe,
    PlanPtr build,
    PredicatePtr probe_filter_predicate,
    PredicatePtr build_filter_predicate,
    const std::vector<AttributeReferencePtr> &probe_attributes,
    const std::vector<AttributeReferencePtr> &build_attributes,
    const bool is_basic_join,
    JoinInfo *info) {
  if (probe_attributes.size() != 1) {
    return false;
  }
  DCHECK_EQ(1u, build_attributes.size());

  auto &eqclass = info->join_attribute_class;
  if (!eqclass.empty() &&
      !ContainsKey(eqclass, probe_attributes.front()->id()) &&
      !ContainsKey(eqclass, build_attributes.front()->id())) {
    return false;
  }

  eqclass.emplace(probe_attributes.front()->id());
  eqclass.emplace(build_attributes.front()->id());

  info->has_basic_join |= is_basic_join;

  if (probe_filter_predicate == nullptr) {
    // Try fuse probe side selection
    SelectionPtr probe_selection;
    if (SomeSelection::MatchesWithConditionalCast(probe, &probe_selection) &&
        probe_selection->selection_type() == Selection::kBasic) {
      DCHECK(SubsetOfExpressions(probe_selection->getOutputAttributes(),
                                 probe_selection->input()->getOutputAttributes()));

      probe = probe_selection->input();
      probe_filter_predicate = probe_selection->filter_predicate();
    }
  }

  if (build_filter_predicate == nullptr) {
    // Try fuse build side selection
    SelectionPtr build_selection;
    if (SomeSelection::MatchesWithConditionalCast(build, &build_selection) &&
        build_selection->selection_type() == Selection::kBasic) {
      DCHECK(SubsetOfExpressions(build_selection->getOutputAttributes(),
                                 build_selection->input()->getOutputAttributes()));

      build = build_selection->input();
      build_filter_predicate = build_selection->filter_predicate();
    }
  }

  bool unnest_probe = false;
  EquiJoinPtr probe_equi_join;
  if (probe_filter_predicate == nullptr &&
      SomeEquiJoin::MatchesWithConditionalCast(probe, &probe_equi_join) &&
      probe_equi_join->join_type() == EquiJoin::kHashInnerJoin) {
    unnest_probe = collectEquiJoin(probe_equi_join->probe(),
                                   probe_equi_join->build(),
                                   nullptr /* probe_filter_predicate */,
                                   nullptr /* build_filter_predicate */,
                                   probe_equi_join->probe_attributes(),
                                   probe_equi_join->build_attributes(),
                                   true /* is_basic_join */,
                                   info);
  }
  if (!unnest_probe) {
    info->plans.emplace_back(probe);
    info->join_attributes.emplace_back(probe_attributes.front());
    info->filter_predicates.emplace_back(probe_filter_predicate);
  }

  bool unnest_build = false;
  EquiJoinPtr build_equi_join;
  if (build_filter_predicate == nullptr &&
      SomeEquiJoin::MatchesWithConditionalCast(build, &build_equi_join) &&
      build_equi_join->join_type() == EquiJoin::kHashInnerJoin) {
    unnest_build = collectEquiJoin(build_equi_join->probe(),
                                   build_equi_join->build(),
                                   nullptr /* probe_filter_predicate */,
                                   nullptr /* build_filter_predicate */,
                                   build_equi_join->probe_attributes(),
                                   build_equi_join->build_attributes(),
                                   true /* is_basic_join */,
                                   info);
  } else {
    info->plans.emplace_back(build);
    info->join_attributes.emplace_back(build_attributes.front());
    info->filter_predicates.emplace_back(build_filter_predicate);
  }

  return true;
}

PlanPtr SpecializeMultiwayJoinAggregate::rewriteAggregateExpressions(
    const MultiwayEquiJoinAggregatePtr &multiway_join_aggr) const {
  const auto &inputs = multiway_join_aggr->inputs();
  std::vector<std::vector<AttributeReferencePtr>> inputs_output_attributes(inputs.size());
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    inputs_output_attributes[i] = inputs[i]->getOutputAttributes();
  }

  const int kAnyInput = -1;
  const auto &join_attributes = multiway_join_aggr->join_attributes();
  auto aggregate_expressions = multiway_join_aggr->aggregate_expressions();
  std::vector<int> aggr_expr_to_inputs_mapping(aggregate_expressions.size(), kAnyInput);
  std::vector<int> non_join_aggr_expr_to_inputs_mapping;
  non_join_aggr_expr_to_inputs_mapping.reserve(4u);
  for (std::size_t i = 0; i < aggregate_expressions.size(); ++i) {
    const auto aggr_referenced_attributes = aggregate_expressions[i]->getReferencedAttributes();
    DCHECK_EQ(1u, aggr_referenced_attributes.size());

    const ExprId expr_id = aggr_referenced_attributes.front()->id();
    if (ContainsExprId(join_attributes, expr_id)) {
      continue;
    }

    for (std::size_t j = 0; j < inputs_output_attributes.size(); ++j) {
      if (ContainsExprId(inputs_output_attributes[j], expr_id)) {
        aggr_expr_to_inputs_mapping[i] = j;
        non_join_aggr_expr_to_inputs_mapping.push_back(j);
      }
    }
  }

  int substitute_input_index = kAnyInput;
  if (non_join_aggr_expr_to_inputs_mapping.size() == 1u) {
    substitute_input_index = non_join_aggr_expr_to_inputs_mapping.front();
  } else {
    std::size_t min_num_tuples = std::numeric_limits<std::size_t>::max();
    for (std::size_t i = 0; i < join_attributes.size(); ++i) {
      const Attribute *attr = cost_model_.findSourceAttribute(join_attributes[i]->id(), inputs[i]);
      const std::size_t num_tuples = attr->getParentRelation().getNumTuples();

      if (num_tuples < min_num_tuples) {
        substitute_input_index = i;
      }
    }
  }
  DCHECK_NE(substitute_input_index, kAnyInput);

  for (std::size_t i = 0; i < aggr_expr_to_inputs_mapping.size(); ++i) {
    if (aggr_expr_to_inputs_mapping[i] == kAnyInput) {
      AliasPtr alias;
      CHECK(SomeAlias::MatchesWithConditionalCast(aggregate_expressions[i], &alias));
      AggregateFunctionPtr aggr_func;
      CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(), &aggr_func));

      aggregate_expressions[i] = Alias::Create(
          aggr_func->copyWithNewChildren({ join_attributes[substitute_input_index] }),
          alias->id(), alias->name());
    }
  }

  return MultiwayEquiJoinAggregate::Create(inputs, join_attributes, aggregate_expressions,
                                           multiway_join_aggr->filter_predicates());
}

}  // namespace optimizer
}  // namespace project
