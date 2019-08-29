#include "optimizer/rules/CollapseSelection.hpp"

#include <iostream>
#include <vector>

#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr CollapseSelection::apply(const PlanPtr &input) {
  DCHECK(input != nullptr);
  return applyInternal(input, true);
}

PlanPtr CollapseSelection::applyInternal(const PlanPtr &input,
                                         const bool is_output_fixed) {
  // TODO(robin-team): Change this when adding certain plan nodes (e.g. Union).
  const bool is_child_output_fixed =
      (input->getPlanType() == PlanType::kTopLevelPlan);

  std::vector<PlanPtr> new_children;
  new_children.reserve(input->getNumChildren());
  for (const PlanPtr &child : input->children()) {
    new_children.emplace_back(applyInternal(child, is_child_output_fixed));
  }

  if (new_children == input->children()) {
    return applyToNode(input, is_output_fixed);
  } else {
    return applyToNode(input->copyWithNewChildren(new_children), is_output_fixed);
  }
}


PlanPtr CollapseSelection::applyToNode(const PlanPtr &node,
                                       const bool is_output_fixed) {
  SelectionPtr selection;
  if (!SomeSelection::MatchesWithConditionalCast(node, &selection) ||
      selection->selection_type() != Selection::kBasic) {
    return node;
  }

  const PlanPtr &child = selection->input();

  SelectionPtr child_selection;
  if (SomeSelection::MatchesWithConditionalCast(child, &child_selection) &&
      child_selection->selection_type() == Selection::kBasic) {
    std::vector<ScalarPtr> project_expressions = selection->project_expressions();
    PredicatePtr filter_predicate = selection->filter_predicate();

    PullUpProjectExpressions(child_selection->project_expressions(),
                             &project_expressions,
                             &filter_predicate);

    const PredicatePtr &child_filter_predicate = child_selection->filter_predicate();
    if (filter_predicate != nullptr) {
      if (child_filter_predicate != nullptr) {
        filter_predicate = CreateConjunctivePredicate(
            { filter_predicate,  child_filter_predicate });
      }
    } else {
      filter_predicate = child_filter_predicate;
    }

    return Selection::Create(child_selection->input(),
                             project_expressions,
                             filter_predicate,
                             child_selection->selection_type());
  }

  EquiJoinPtr child_equi_join;
  if (selection->filter_predicate() == nullptr &&
      SomeEquiJoin::MatchesWithConditionalCast(child, &child_equi_join)) {
    std::vector<ScalarPtr> project_expressions = selection->project_expressions();

    PullUpProjectExpressions(child_equi_join->project_expressions(),
                             &project_expressions,
                             nullptr /* target_predicate */);

    return EquiJoin::Create(child_equi_join->probe(),
                            child_equi_join->build(),
                            child_equi_join->probe_attributes(),
                            child_equi_join->build_attributes(),
                            project_expressions,
                            child_equi_join->probe_filter_predicate(),
                            child_equi_join->build_filter_predicate(),
                            child_equi_join->join_type());
  }

  if (!is_output_fixed &&
      selection->filter_predicate() == nullptr &&
      SubsetOfExpressions(selection->getOutputAttributes(),
                          child->getOutputAttributes())) {
    return child;
  }

  return node;
}

}  // namespace optimizer
}  // namespace project
