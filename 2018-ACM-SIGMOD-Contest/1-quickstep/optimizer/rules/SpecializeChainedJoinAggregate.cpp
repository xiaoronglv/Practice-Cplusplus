#include "optimizer/rules/SpecializeChainedJoinAggregate.hpp"

#include <memory>
#include <unordered_set>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AggregateFunction.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/ChainedJoinAggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr SpecializeChainedJoinAggregate::applyToNode(const PlanPtr &node) {
  AggregatePtr aggregate;
  EquiJoinPtr equi_join;
  if (!SomeAggregate::MatchesWithConditionalCast(node, &aggregate) ||
      !SomeEquiJoin::MatchesWithConditionalCast(aggregate->input(), &equi_join) ||
      equi_join->join_type() != EquiJoin::kHashInnerJoin ||
      equi_join->build_attributes().size() > 1u) {
    return node;
  }

  DCHECK(equi_join->build_filter_predicate() == nullptr);
  DCHECK(equi_join->probe_filter_predicate() == nullptr);

  EquiJoinPtr equi_join_child;
  PlanPtr probe;
  AttributeReferencePtr probe_attribute;
  if (SomeEquiJoin::MatchesWithConditionalCast(equi_join->build(), &equi_join_child)) {
    probe = equi_join->probe();
    probe_attribute = equi_join->probe_attributes().front();
  }

  if (equi_join_child) {
    if (equi_join->probe()->getPlanType() == PlanType::kEquiJoin) {
      // Bushy tree.
      return node;
    }
  } else if (SomeEquiJoin::MatchesWithConditionalCast(equi_join->probe(), &equi_join_child)) {
    probe = equi_join->build();
    probe_attribute = equi_join->build_attributes().front();
  } else {
    // Single join.
    return node;
  }
  DCHECK(equi_join_child->build_filter_predicate() == nullptr);
  DCHECK(equi_join_child->probe_filter_predicate() == nullptr);

  const auto equi_join_child_output_attributes = equi_join_child->getOutputAttributes();
  if (equi_join_child_output_attributes.size() > 1u ||
      equi_join_child->join_type() != EquiJoin::kHashInnerJoin ||
      equi_join_child->build_attributes().size() > 1u) {
    return node;
  }

  const AttributeReferencePtr &build_attribute = equi_join_child_output_attributes.front();
  const ExprId build_attribute_expr_id = build_attribute->id();
  if (build_attribute_expr_id == equi_join_child->build_attributes().front()->id() ||
      build_attribute_expr_id == equi_join_child->probe_attributes().front()->id()) {
    // Multiway join.
    return node;
  }

  std::vector<PlanPtr> inputs(3u);
  std::vector<ChainedJoinAggregate::AttributePair> join_attribute_pairs(2u);
  if (SubsetOfExpressions(equi_join_child_output_attributes,
                          equi_join_child->probe()->getOutputAttributes())) {
    inputs[0] = equi_join_child->build();
    inputs[1] = equi_join_child->probe();

    join_attribute_pairs[0].first = equi_join_child->build_attributes().front();
    join_attribute_pairs[0].second = equi_join_child->probe_attributes().front();
  } else if (SubsetOfExpressions(equi_join_child_output_attributes,
                                 equi_join_child->build()->getOutputAttributes())) {
    inputs[0] = equi_join_child->probe();
    inputs[1] = equi_join_child->build();

    join_attribute_pairs[0].first = equi_join_child->probe_attributes().front();
    join_attribute_pairs[0].second = equi_join_child->build_attributes().front();
  } else {
    return node;
  }

  inputs[2] = probe;

  join_attribute_pairs[1].first = build_attribute;
  join_attribute_pairs[1].second = probe_attribute;

  std::unordered_set<ExprId> probe_output_attribute_expr_ids;
  probe_output_attribute_expr_ids.reserve(5u);

  for (const auto &attr : probe->getOutputAttributes()) {
    probe_output_attribute_expr_ids.emplace(attr->id());
  }

  auto aggregate_expressions = aggregate->aggregate_expressions();
  for (auto &aggr_expr : aggregate_expressions) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(aggr_expr, &alias));
    AggregateFunctionPtr aggr_func;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(), &aggr_func));
    AttributeReferencePtr attr;
    CHECK(SomeAttributeReference::MatchesWithConditionalCast(aggr_func->argument(), &attr));

    const ExprId attr_expr_id = attr->id();
    const auto it = probe_output_attribute_expr_ids.find(attr_expr_id);
    if (it != probe_output_attribute_expr_ids.end()) {
      continue;
    }

    if (attr_expr_id != build_attribute_expr_id) {
      return node;
    }

    aggr_expr = Alias::Create(aggr_func->copyWithNewChildren({ probe_attribute }),
                              alias->id(), alias->name());
  }

  std::vector<PredicatePtr> filter_predicates(inputs.size(), nullptr);

  for (int i = 0; i < inputs.size(); ++i) {
    SelectionPtr selection;
    if (SomeSelection::MatchesWithConditionalCast(inputs[i], &selection) &&
        selection->selection_type() == Selection::kBasic) {
      DCHECK(SubsetOfExpressions(selection->getOutputAttributes(),
                                 selection->input()->getOutputAttributes()));
      inputs[i] = selection->input();
      filter_predicates[i] = selection->filter_predicate();
    }
  }

  return ChainedJoinAggregate::Create(inputs, join_attribute_pairs,
                                      aggregate_expressions,
                                      filter_predicates);
}

}  // namespace optimizer
}  // namespace project
