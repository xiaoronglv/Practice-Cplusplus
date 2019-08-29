#include "optimizer/rules/FuseOperators.hpp"

#include <cstddef>
#include <memory>
#include <utility>

#include "optimizer/Aggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Selection.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr FuseOperators::applyToNode(const PlanPtr &node) {
  switch (node->getPlanType()) {
    case PlanType::kAggregate:
      return applyToAggregate(std::static_pointer_cast<const Aggregate>(node));
    case PlanType::kEquiJoin:
      return applyToEquiJoin(std::static_pointer_cast<const EquiJoin>(node));
    default:
      break;
  }
  return node;
}

PlanPtr FuseOperators::applyToAggregate(const AggregatePtr &aggregate) const {
  const PlanPtr &input = aggregate->input();
  switch (input->getPlanType()) {
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(input);
      return fuseAggregateSelection(aggregate, selection);
    }
    default:
      break;
  }
  return aggregate;
}

PlanPtr FuseOperators::applyToEquiJoin(const EquiJoinPtr &equi_join) const {
  if (equi_join->join_type() != EquiJoin::kHashInnerJoin &&
      equi_join->join_type() != EquiJoin::kLeftSemiJoin) {
    return equi_join;
  }

  DCHECK(equi_join->probe_filter_predicate() == nullptr);
  DCHECK(equi_join->build_filter_predicate() == nullptr);

  PlanPtr probe = equi_join->probe();
  PlanPtr build = equi_join->build();

  PredicatePtr probe_filter_predicate;
  PredicatePtr build_filter_predicate;

  SelectionPtr probe_selection;
  if (SomeSelection::MatchesWithConditionalCast(probe, &probe_selection) &&
      probe_selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(probe_selection->getOutputAttributes(),
                               probe_selection->input()->getOutputAttributes()));

    probe = probe_selection->input();
    probe_filter_predicate = probe_selection->filter_predicate();
  }

  SelectionPtr build_selection;
  if (SomeSelection::MatchesWithConditionalCast(build, &build_selection) &&
      build_selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(build_selection->getOutputAttributes(),
                               build_selection->input()->getOutputAttributes()));

    build = build_selection->input();
    build_filter_predicate = build_selection->filter_predicate();
  }

  auto probe_attributes = equi_join->probe_attributes();
  auto build_attributes = equi_join->build_attributes();

  EquiJoin::JoinType join_type = EquiJoin::kSortMergeJoin;

  const std::size_t probe_cardinality =
      cost_model_.estimateCardinality(equi_join->probe());
  const std::size_t build_cardinality =
      cost_model_.estimateCardinality(equi_join->build());

  if (probe_cardinality <= kHashJoinCardinalityThreshold ||
      build_cardinality <= kHashJoinCardinalityThreshold) {
    if (build_cardinality > kHashJoinCardinalityThreshold) {
      std::swap(probe, build);
      std::swap(probe_attributes, build_attributes);
      std::swap(probe_filter_predicate, build_filter_predicate);
    }
    join_type = EquiJoin::kHashInnerJoin;
  }

  return EquiJoin::Create(probe,
                          build,
                          probe_attributes,
                          build_attributes,
                          equi_join->project_expressions(),
                          probe_filter_predicate,
                          build_filter_predicate,
                          join_type);
}

PlanPtr FuseOperators::fuseAggregateSelection(
    const AggregatePtr &aggregate, const SelectionPtr &selection) const {
  if (selection->selection_type() != Selection::kBasic) {
    return aggregate;
  }

  DCHECK(SubsetOfExpressions(selection->getOutputAttributes(),
                             selection->input()->getOutputAttributes()));

  return Aggregate::Create(selection->input(),
                           aggregate->aggregate_expressions(),
                           selection->filter_predicate());
}

}  // namespace optimizer
}  // namespace project
