#include "optimizer/rules/SpecializePrimaryKeyIndexJoin.hpp"

#include <cstddef>

#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "storage/Attribute.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr SpecializePrimaryKeyIndexJoin::applyToNode(const PlanPtr &node) {
  EquiJoinPtr equi_join;
  if (!SomeEquiJoin::MatchesWithConditionalCast(node, &equi_join) ||
      equi_join->join_type() != EquiJoin::kHashInnerJoin) {
    return node;
  }

  TableReferencePtr table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(equi_join->build(),
                                                      &table_reference)) {
    return node;
  }

  const std::size_t probe_side_card =
      cost_model_.estimateCardinality(equi_join->probe());
  if (probe_side_card > cardinality_threshold_) {
    return node;
  }

  if (equi_join->build_attributes().size() > 1) {
    return node;
  }

  const Attribute *build_attribute =
      cost_model_.findSourceAttribute(equi_join->build_attributes().front()->id(),
                                      table_reference);

  if (build_attribute == nullptr) {
    return node;
  }

  const IndexManager &index_manager =
      build_attribute->getParentRelation().getIndexManager();

  if (!index_manager.hasPrimaryKeyIndex(build_attribute->id())) {
    return node;
  }

  DCHECK(equi_join->probe_filter_predicate() == nullptr);
  DCHECK(equi_join->build_filter_predicate() == nullptr);

  PlanPtr probe = equi_join->probe();
  PredicatePtr probe_filter_predicate;

  SelectionPtr selection;
  if (SomeSelection::MatchesWithConditionalCast(equi_join->probe(), &selection) &&
      selection->selection_type() == Selection::kBasic) {
    DCHECK(SubsetOfExpressions(selection->getOutputAttributes(),
                               selection->input()->getOutputAttributes()));

    probe = selection->input();
    probe_filter_predicate = selection->filter_predicate();
  }

  return EquiJoin::Create(probe,
                          equi_join->build(),
                          equi_join->probe_attributes(),
                          equi_join->build_attributes(),
                          equi_join->project_expressions(),
                          probe_filter_predicate,
                          equi_join->build_filter_predicate(),
                          EquiJoin::kPrimaryKeyIndexJoin);
}

}  // namespace optimizer
}  // namespace project
