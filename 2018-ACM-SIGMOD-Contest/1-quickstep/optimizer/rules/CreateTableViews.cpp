#include "optimizer/rules/CreateTableViews.hpp"

#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "storage/Attribute.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/Relation.hpp"
#include "storage/RelationStatistics.hpp"

namespace project {
namespace optimizer {

PlanPtr CreateTableViews::applyToNode(const PlanPtr &node) {
  SelectionPtr selection;
  if (!SomeSelection::MatchesWithConditionalCast(node, &selection) ||
      selection->filter_predicate() == nullptr) {
    return node;
  }

  for (const auto &scalar : selection->project_expressions()) {
    if (scalar->getExpressionType() != ExpressionType::kAttributeReference) {
      return node;
    }
  }

  PlanPtr input = selection->input();

  TableReferencePtr table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(input, &table_reference)) {
    return node;
  }
  const RelationStatistics &stat = table_reference->relation().getStatistics();

  const std::vector<PredicatePtr> predicates =
      GetConjunctivePredicates(selection->filter_predicate());
  std::vector<PredicatePtr> residual_predicates;

  for (const auto &predicate : predicates) {
    const PlanPtr output = tryCreate(predicate, input, stat);
    if (output != nullptr) {
      input = output;
    } else {
      residual_predicates.emplace_back(predicate);
    }
  }

  if (input == selection->input()) {
    return node;
  }

  if (residual_predicates.empty()) {
    return input;
  }

  return Selection::Create(input,
                           selection->project_expressions(),
                           CreateConjunctivePredicate(residual_predicates),
                           selection->selection_type());
}

PlanPtr CreateTableViews::tryCreate(const PredicatePtr &predicate,
                                    const PlanPtr &node,
                                    const RelationStatistics &stat) const {
  ComparisonPtr comparison;
  if (!SomeComparison::MatchesWithConditionalCast(predicate, &comparison)) {
    return nullptr;
  }

  AttributeReferencePtr attribute;
  if (!SomeAttributeReference::MatchesWithConditionalCast(comparison->left(), &attribute) ||
      comparison->right()->getExpressionType() != ExpressionType::kScalarLiteral) {
    return nullptr;
  }

  const Attribute *source = cost_model_.findSourceAttribute(attribute->id(), node);
  if (source == nullptr) {
    return nullptr;
  }

  const AttributeStatistics &attr_stat = stat.getAttributeStatistics(source->id());

  if (attr_stat.getSortOrder() != SortOrder::kAscendant &&
      attr_stat.getSortOrder() != SortOrder::kDescendant) {
    return nullptr;
  }

  return TableView::Create(node, comparison, attr_stat.getSortOrder());
}

}  // namespace optimizer
}  // namespace project
