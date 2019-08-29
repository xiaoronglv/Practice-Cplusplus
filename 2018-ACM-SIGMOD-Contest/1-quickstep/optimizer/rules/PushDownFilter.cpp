#include "optimizer/rules/PushDownFilter.hpp"

#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Filter.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "utility/ContainerUtil.hpp"

namespace project {
namespace optimizer {

PlanPtr PushDownFilter::applyToNode(const PlanPtr &node) {
  FilterPtr filter;

  // The rule is only applied to filters.
  if (!SomeFilter::MatchesWithConditionalCast(node, &filter)) {
    return node;
  }

  std::vector<PredicatePtr> predicates =
      GetConjunctivePredicates(filter->filter_predicate());

  bool chained_filters = false;
  PlanPtr input = filter->input();
  while (SomeFilter::MatchesWithConditionalCast(input, &filter)) {
    InsertAll(GetConjunctivePredicates(filter->filter_predicate()), &predicates);
    input = filter->input();
    chained_filters = true;
  }

  const std::size_t num_children = input->getNumChildren();

  if (num_children == 0) {
    if (chained_filters) {
      return Filter::Create(input, CreateConjunctivePredicate(predicates));
    } else {
      return node;
    }
  }

  const std::vector<PlanPtr> &children = input->children();

  std::vector<std::vector<PredicatePtr>> push_down(num_children);
  std::vector<PredicatePtr> unchanged;

  // We don't need to support outer joins or other special operators.
  // So it is safe to push down a predicate to whatever child.
  for (const PredicatePtr &predicate : predicates) {
    const std::vector<AttributeReferencePtr> reference_attributes =
        predicate->getReferencedAttributes();

    bool can_be_pushed = false;
    for (std::size_t i = 0; i < num_children; ++i) {
      if (SubsetOfExpressions(reference_attributes,
                              children[i]->getOutputAttributes())) {
        push_down[i].emplace_back(predicate);
        can_be_pushed = true;
        break;
      }
    }

    if (!can_be_pushed) {
      unchanged.emplace_back(predicate);
    }
  }

  if (unchanged.size() == predicates.size() && !chained_filters) {
    return node;
  }

  std::vector<PlanPtr> new_children;
  new_children.reserve(num_children);

  for (std::size_t i = 0; i < num_children; ++i) {
    const auto &subset = push_down[i];
    if (subset.empty()) {
      new_children.emplace_back(children[i]);
    } else {
      new_children.emplace_back(
          Filter::Create(children[i], CreateConjunctivePredicate(subset)));
    }
  }

  const PlanPtr output = input->copyWithNewChildren(new_children);

  if (unchanged.empty()) {
    return output;
  } else {
    return Filter::Create(output, CreateConjunctivePredicate(unchanged));
  }
}

}  // namespace optimizer
}  // namespace project
