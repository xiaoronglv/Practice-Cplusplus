#include "optimizer/rules/ReducePredicate.hpp"

#include <cstddef>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "optimizer/Comparison.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/PredicateLiteral.hpp"
#include "optimizer/ScalarLiteral.hpp"
#include "optimizer/Selection.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr ReducePredicate::applyToNode(const PlanPtr &node) {
  switch (node->getPlanType()) {
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(node);

      const PredicatePtr new_filter_predicate =
          reduce(selection->filter_predicate());

      if (new_filter_predicate == selection->filter_predicate()) {
        return node;
      }
      return Selection::Create(selection->input(),
                               selection->project_expressions(),
                               new_filter_predicate,
                               selection->selection_type());
    }
    default:
      break;
  }
  return node;
}

PredicatePtr ReducePredicate::reduce(const PredicatePtr &filter_predicate) const {
  if (filter_predicate == nullptr) {
    return filter_predicate;
  }

  const auto predicates = GetConjunctivePredicates(filter_predicate);
  if (predicates.size() == 1) {
    return filter_predicate;
  }

  std::unordered_map<ExprId, Range> ranges;
  std::vector<PredicatePtr> residual_predicates;

  std::unordered_map<ExprId, AttributeReferencePtr> attribute_to_reference_map;

  for (const auto &predicate : predicates) {
    ComparisonPtr comparison;
    if (!SomeComparison::MatchesWithConditionalCast(predicate, &comparison) ||
        !comparison->isAttributeToLiteralComparison()) {
      residual_predicates.emplace_back(predicate);
      continue;
    }

    AttributeReferencePtr attribute;
    CHECK(SomeAttributeReference::MatchesWithConditionalCast(comparison->left(),
                                                             &attribute));
    attribute_to_reference_map.emplace(attribute->id(), attribute);

    Range range;
    const auto it = ranges.find(attribute->id());
    if (it == ranges.end()) {
      range = Range(0, std::numeric_limits<std::size_t>::max());
    } else {
      range = it->second;
    }
    ranges[attribute->id()] = comparison->reduceRange(attribute->id(), range);
  }

  std::vector<PredicatePtr> new_predicates;

  // First apply equality predicates as they are most selective.
  for (const auto &it : ranges) {
    const Range &range = it.second;
    if (range.size() == 0) {
      return PredicateLiteral::Create(false);
    } else if (range.size() == 1) {
      const ScalarLiteralPtr literal =
          ScalarLiteral::Create(range.begin(),
                                UInt64Type::Instance(),
                                optimizer_context_->nextExprId());

      new_predicates.emplace_back(
          Comparison::Create(ComparisonType::kEqual,
                             attribute_to_reference_map.at(it.first),
                             literal));
    }
  }

  // Then attriute to attribute comparisons.
  InsertAll(residual_predicates, &new_predicates);

  // Finally non-equal comparisons.
  for (const auto &it : ranges) {
    const Range &range = it.second;
    if (range.size() > 1) {
      if (range.begin() != 0) {
        const ScalarLiteralPtr lower_literal =
            ScalarLiteral::Create(range.begin() - 1,
                                  UInt64Type::Instance(),
                                  optimizer_context_->nextExprId());
        new_predicates.emplace_back(
            Comparison::Create(ComparisonType::kGreater,
                               attribute_to_reference_map.at(it.first),
                               lower_literal));
      }

      if (range.end() != std::numeric_limits<std::size_t>::max()) {
        const ScalarLiteralPtr upper_literal =
            ScalarLiteral::Create(range.end(),
                                  UInt64Type::Instance(),
                                  optimizer_context_->nextExprId());

        new_predicates.emplace_back(
            Comparison::Create(ComparisonType::kLess,
                               attribute_to_reference_map.at(it.first),
                               upper_literal));
      }
    }
  }

  return CreateConjunctivePredicate(new_predicates);
}

}  // namespace optimizer
}  // namespace project
