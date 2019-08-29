#include "optimizer/rules/SpecializeSelection.hpp"

#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr SpecializeSelection::applyToNode(const PlanPtr &node) {
  SelectionPtr selection;
  if (!SomeSelection::MatchesWithConditionalCast(node, &selection) ||
      selection->selection_type() != Selection::kBasic) {
    return node;
  }

  if (selection->filter_predicate() == nullptr) {
    return node;
  }

  const auto predicates = GetConjunctivePredicates(selection->filter_predicate());
  PredicatePtr best_predicate = nullptr;
  for (const auto &predicate : predicates) {
    ComparisonPtr comparison;
    if (!SomeComparison::MatchesWithConditionalCast(predicate, &comparison)) {
      continue;
    }

    // TODO(robin-team): Support index scan for non-equal comparison when range
    // is relatively small.
    if (comparison->comparison_type() != ComparisonType::kEqual) {
      continue;
    }

    const ScalarPtr &left = comparison->left();
    const ScalarPtr &right = comparison->right();
    AttributeReferencePtr attribute;
    if (!SomeAttributeReference::MatchesWithConditionalCast(left, &attribute) ||
        right->getExpressionType() != ExpressionType::kScalarLiteral) {
      continue;
    }

    TableReferencePtr table_reference;
    if (!SomeTableReference::MatchesWithConditionalCast(selection->input(),
                                                        &table_reference)) {
      continue;
    }

    const Attribute *target =
        cost_model_.findSourceAttribute(attribute->id(), table_reference);
    DCHECK(target != nullptr);

    const IndexManager &index_manager =
        table_reference->relation().getIndexManager();

    if (!index_manager.hasPrimaryKeyIndex(target->id()) &&
        !index_manager.hasForeignKeyIndex(target->id())) {
      continue;
    }

    best_predicate = comparison;
    break;
  }

  if (best_predicate == nullptr) {
    return node;
  }

  std::vector<PredicatePtr> residual_predicates;
  for (const auto &predicate : predicates) {
    if (predicate != best_predicate) {
      residual_predicates.emplace_back(predicate);
    }
  }

  const std::vector<ScalarPtr> project_expressions =
      CastSharedPtrVector<const Scalar>(selection->input()->getOutputAttributes());

  PlanPtr output = Selection::Create(selection->input(),
                                     project_expressions,
                                     best_predicate,
                                     Selection::kIndexScan);

  if (!residual_predicates.empty()) {
    output = Selection::Create(output,
                               project_expressions,
                               CreateConjunctivePredicate(residual_predicates));
  }

  return output;
}



}  // namespace optimizer
}  // namespace project
