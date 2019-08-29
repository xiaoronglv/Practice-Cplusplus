#include "optimizer/rules/GenerateSelection.hpp"

#include "optimizer/Filter.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Project.hpp"
#include "optimizer/Selection.hpp"
#include "utility/MemoryUtil.hpp"

namespace project {
namespace optimizer {

PlanPtr GenerateSelection::applyToNode(const PlanPtr &node) {
  ProjectPtr project;
  FilterPtr filter;

  if (SomeProject::MatchesWithConditionalCast(node, &project)) {
    PlanPtr input = project->input();
    PredicatePtr filter_predicate = nullptr;

    if (SomeFilter::MatchesWithConditionalCast(project->input(), &filter)) {
      input = filter->input();
      filter_predicate = filter->filter_predicate();
    }

    return Selection::Create(
        input,
        CastSharedPtrVector<const Scalar>(project->project_attributes()),
        filter_predicate);
  }

  if (SomeFilter::MatchesWithConditionalCast(node, &filter)) {
    return Selection::Create(
        filter->input(),
        CastSharedPtrVector<const Scalar>(filter->input()->getOutputAttributes()),
        filter->filter_predicate());
  }

  return node;
}

}  // namespace optimizer
}  // namespace project
