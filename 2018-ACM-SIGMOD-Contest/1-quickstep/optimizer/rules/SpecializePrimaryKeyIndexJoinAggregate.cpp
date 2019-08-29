#include "optimizer/rules/SpecializePrimaryKeyIndexJoinAggregate.hpp"

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

PlanPtr SpecializePrimaryKeyIndexJoinAggregate::applyToNode(const PlanPtr &node) {
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

  TableReferencePtr build_table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(equi_join->build(),
                                                      &build_table_reference)) {
    return node;
  }

  const Attribute *build_attribute =
      cost_model_.findSourceAttribute(equi_join->build_attributes().front()->id(),
                                      build_table_reference);

  if (build_attribute == nullptr) {
    return node;
  }

  const IndexManager &build_index_manager =
      build_attribute->getParentRelation().getIndexManager();

  if (!build_index_manager.hasPrimaryKeyIndex(build_attribute->id())) {
    return node;
  }

  const Attribute *probe_source_attribute =
      cost_model_.findSourceAttribute(equi_join->probe_attributes().front()->id(),
                                      equi_join->probe());

  if (probe_source_attribute == nullptr) {
    return node;
  }

  // TODO(robin-team): Handle the non foreign-key case.
  if (!database_.isPrimaryKeyForeignKey(build_attribute, probe_source_attribute)) {
    return node;
  }

  PlanPtr probe = equi_join->probe();
  PredicatePtr probe_filter_predicate = nullptr;

  SelectionPtr selection;
  if (SomeSelection::MatchesWithConditionalCast(probe, &selection) &&
      selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(selection->getOutputAttributes(),
                               selection->input()->getOutputAttributes()));
    probe = selection->input();
    probe_filter_predicate = selection->filter_predicate();
  }

  const bool use_fk_index =
      canUseForeignKeyIndex(equi_join, probe, probe_filter_predicate);

  const EquiJoinAggregate::JoinType join_type =
      use_fk_index ? EquiJoinAggregate::kForeignKeyIndexPrimaryKeyIndex
                   : EquiJoinAggregate::kForeignKeyPrimaryKeyIndex;

  return EquiJoinAggregate::Create(probe,
                                   equi_join->build(),
                                   equi_join->probe_attributes(),
                                   equi_join->build_attributes(),
                                   aggregate->aggregate_expressions(),
                                   probe_filter_predicate,
                                   nullptr /* build_filter_predicate */,
                                   join_type);
}

bool SpecializePrimaryKeyIndexJoinAggregate::canUseForeignKeyIndex(
    const EquiJoinPtr &equi_join,
    const PlanPtr &probe,
    const PredicatePtr &predicate) const {
  TableReferencePtr probe_table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(probe, &probe_table_reference)) {
    return false;
  }

  const auto &probe_attribute_ref = equi_join->probe_attributes().front();
  const Attribute *probe_attribute =
      cost_model_.findSourceAttribute(probe_attribute_ref->id(),
                                      probe_table_reference);

  if (probe_attribute == nullptr) {
    return false;
  }

  const IndexManager &probe_index_manager =
      probe_attribute->getParentRelation().getIndexManager();

  if (!probe_index_manager.hasForeignKeyIndex(probe_attribute->id())) {
    return false;
  }

  if (predicate != nullptr) {
    UnorderedAttributeSet referenced_attributes;
    InsertAll(predicate->getReferencedAttributes(), &referenced_attributes);
    if (referenced_attributes.size() > 1) {
      return false;
    }
    if (referenced_attributes.size() == 1 &&
        (*referenced_attributes.begin())->id() != probe_attribute_ref->id()) {
      return false;
    }
  }

  UnorderedAttributeSet output_attributes;
  InsertAll(equi_join->getOutputAttributes(), &output_attributes);
  EraseAll(&output_attributes, equi_join->build()->getOutputAttributes());

  if (output_attributes.size() > 1) {
    return false;
  }
  if (output_attributes.size() == 1 &&
      (*output_attributes.begin())->id() != probe_attribute_ref->id()) {
    return false;
  }

  return true;
}

}  // namespace optimizer
}  // namespace project

