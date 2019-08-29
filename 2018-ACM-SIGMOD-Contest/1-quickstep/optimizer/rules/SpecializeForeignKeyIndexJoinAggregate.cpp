#include "optimizer/rules/SpecializeForeignKeyIndexJoinAggregate.hpp"

#include <cstddef>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Aggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "storage/Attribute.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "utility/ContainerUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr SpecializeForeignKeyIndexJoinAggregate::applyToNode(const PlanPtr &node) {
  AggregatePtr aggregate;
  if (!SomeAggregate::MatchesWithConditionalCast(node, &aggregate) ||
      aggregate->filter_predicate() != nullptr) {
    return node;
  }

  EquiJoinPtr equi_join;
  if (!SomeEquiJoin::MatchesWithConditionalCast(aggregate->input(), &equi_join) ||
      equi_join->join_type() != EquiJoin::kHashInnerJoin ||
      equi_join->build_attributes().size() != 1) {
    return node;
  }
  DCHECK_EQ(1u, equi_join->probe_attributes().size());

  const auto probe_attribute = equi_join->probe_attributes().front();
  const auto build_attribute = equi_join->build_attributes().front();

  PlanPtr probe = equi_join->probe();
  PlanPtr build = equi_join->build();

  // Fuse probe-side selection.
  PredicatePtr probe_filter_predicate;
  SelectionPtr probe_selection;
  if (SomeSelection::MatchesWithConditionalCast(probe, &probe_selection) &&
      probe_selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(probe_selection->getOutputAttributes(),
                               probe_selection->input()->getOutputAttributes()));
    probe_filter_predicate = probe_selection->filter_predicate();
    probe = probe_selection->input();
  }

  // Fuse build-side selection.
  PredicatePtr build_filter_predicate;
  SelectionPtr build_selection;
  if (SomeSelection::MatchesWithConditionalCast(build, &build_selection) &&
      build_selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(build_selection->getOutputAttributes(),
                               build_selection->input()->getOutputAttributes()));
    build_filter_predicate = build_selection->filter_predicate();
    build = build_selection->input();
  }

  TableReferencePtr build_table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(build, &build_table_reference)) {
    return node;
  }

  if (build_filter_predicate != nullptr) {
    UnorderedAttributeSet referenced_attributes;
    InsertAll(build_filter_predicate->getReferencedAttributes(),
              &referenced_attributes);
    if (referenced_attributes.size() != 1 ||
        (*referenced_attributes.begin())->id() != build_attribute->id()) {
      return node;
    }
  }

  const Attribute *index_attribute =
      cost_model_.findSourceAttribute(build_attribute->id(), build_table_reference);

  if (index_attribute == nullptr) {
    return node;
  }

  const IndexManager &index_manager =
      index_attribute->getParentRelation().getIndexManager();

  if (!index_manager.hasForeignKeyIndex(index_attribute->id())) {
    return node;
  }

  UnorderedAttributeSet output_attributes;
  InsertAll(equi_join->getOutputAttributes(), &output_attributes);

  for (const auto &attr : build->getOutputAttributes()) {
    if (output_attributes.find(attr) != output_attributes.end()) {
      return node;
    }
  }

  return EquiJoinAggregate::Create(probe,
                                   build,
                                   equi_join->probe_attributes(),
                                   equi_join->build_attributes(),
                                   aggregate->aggregate_expressions(),
                                   probe_filter_predicate,
                                   build_filter_predicate,
                                   EquiJoinAggregate::kBuildSideForeignKeyIndex);
}

}  // namespace optimizer
}  // namespace project

