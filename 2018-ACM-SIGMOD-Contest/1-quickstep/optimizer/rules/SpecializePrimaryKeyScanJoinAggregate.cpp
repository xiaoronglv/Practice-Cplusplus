#include "optimizer/rules/SpecializePrimaryKeyScanJoinAggregate.hpp"

#include <cstddef>
#include <memory>
#include <vector>

#include "operators/expressions/ComparisonType.hpp"
#include "optimizer/Aggregate.hpp"
#include "optimizer/AggregateFunction.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "storage/StorageTypedefs.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr SpecializePrimaryKeyScanJoinAggregate::applyToNode(const PlanPtr &node) {
  AggregatePtr aggregate;
  if (!SomeAggregate::MatchesWithConditionalCast(node, &aggregate) ||
      aggregate->filter_predicate() != nullptr) {
    return node;
  }

  EquiJoinPtr equi_join;
  if (!SomeEquiJoin::MatchesWithConditionalCast(aggregate->input(), &equi_join) ||
      equi_join->join_type() != EquiJoin::kHashInnerJoin ||
      equi_join->build_attributes().size() != 1u) {
    return node;
  }
  const auto &probe_attributes = equi_join->probe_attributes();
  DCHECK_EQ(1u, probe_attributes.size());

  SelectionPtr build_selection;
  if (!SomeSelection::MatchesWithConditionalCast(equi_join->build(),
                                                 &build_selection) ||
      build_selection->filter_predicate() == nullptr) {
    return node;
  }
  DCHECK(SubsetOfExpressions(build_selection->getOutputAttributes(),
                             build_selection->input()->getOutputAttributes()));

  TableReferencePtr build_selection_table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(build_selection->input(),
                                                      &build_selection_table_reference)) {
    return node;
  }

  bool predicate_on_pk = false;

  const PredicatePtr &build_filter_predicate = build_selection->filter_predicate();
  for (const auto &predicate : GetConjunctivePredicates(build_filter_predicate)) {
    ComparisonPtr comparison;
    CHECK(SomeComparison::MatchesWithConditionalCast(predicate, &comparison));
    if (!comparison->isAttributeToLiteralComparison() ||
        comparison->comparison_type() != ComparisonType::kEqual) {
      continue;
    }

    if (cost_model_.impliesUniqueAttributes(build_selection_table_reference,
                                            { comparison->left()->getAttribute() })) {
      predicate_on_pk = true;
      break;
    }
  }

  if (!predicate_on_pk) {
    return node;
  }

  PlanPtr probe = equi_join->probe();
  PredicatePtr probe_filter_predicate = nullptr;

  SelectionPtr probe_selection;
  if (SomeSelection::MatchesWithConditionalCast(probe, &probe_selection) &&
      probe_selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(probe_selection->getOutputAttributes(),
                               probe_selection->input()->getOutputAttributes()));
    probe = probe_selection->input();
    probe_filter_predicate = probe_selection->filter_predicate();
  }

  const auto &build_attributes = equi_join->build_attributes();
  auto aggregate_expressions = aggregate->aggregate_expressions();
  rewriteAggregateExpressions(probe_attributes.front()->id(),
                              build_attributes.front(),
                              &aggregate_expressions);

  return EquiJoinAggregate::Create(probe,
                                   build_selection_table_reference,
                                   probe_attributes,
                                   build_attributes,
                                   aggregate_expressions,
                                   probe_filter_predicate,
                                   build_filter_predicate,
                                   EquiJoinAggregate::kForeignKeyPrimaryKeyScan);
}

void SpecializePrimaryKeyScanJoinAggregate::rewriteAggregateExpressions(
    const ExprId probe_attribute, const AttributeReferencePtr &build_attribute,
    std::vector<ScalarPtr> *aggregate_expressions) {
  for (auto &aggregate_expression : *aggregate_expressions) {
    const auto aggr_referenced_attributes = aggregate_expression->getReferencedAttributes();
    DCHECK_EQ(1u, aggr_referenced_attributes.size());

    if (aggr_referenced_attributes.front()->id() != probe_attribute) {
      continue;
    }

    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(aggregate_expression, &alias));
    AggregateFunctionPtr aggr_func;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(), &aggr_func));

    aggregate_expression = Alias::Create(
        aggr_func->copyWithNewChildren({ build_attribute }), alias->id(), alias->name());
  }
}

}  // namespace optimizer
}  // namespace project
