#include "optimizer/rules/ReduceSemiJoin.hpp"

#include <unordered_map>

#include "optimizer/EquiJoin.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/rules/UpdateExpression.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr ReduceSemiJoin::applyToNode(const PlanPtr &node) {
  EquiJoinPtr equi_join;
  if (!SomeEquiJoin::MatchesWithConditionalCast(node, &equi_join) ||
      equi_join->build_attributes().size() != 1) {
    return node;
  }

  if (!SubsetOfExpressions(equi_join->getOutputAttributes(),
                           equi_join->probe()->getOutputAttributes())) {
    return node;
  }

  if (!cost_model_.impliesUniqueAttributes(equi_join->build(),
                                           equi_join->build_attributes())) {
    return node;
  }

  const PlanPtr &probe = equi_join->probe();
  PlanPtr build = equi_join->build();

  PredicatePtr filter_predicate = nullptr;
  SelectionPtr selection;
  if (SomeSelection::MatchesWithConditionalCast(build, &selection)) {
    if (selection->filter_predicate() == nullptr) {
      build = selection->input();
    } else {
      const auto referenced_attributes =
          selection->filter_predicate()->getReferencedAttributes();
      if (SubsetOfExpressions(referenced_attributes,
                              equi_join->build_attributes())) {
        build = selection->input();
        filter_predicate = selection->filter_predicate();
      }
    }
  }

  if (build->getPlanType() != PlanType::kTableReference) {
    return node;
  }

  const Attribute *probe_attr = cost_model_.findSourceAttribute(
      equi_join->probe_attributes().front()->id(), probe);
  const Attribute *build_attr = cost_model_.findSourceAttribute(
      equi_join->build_attributes().front()->id(), build);

  if (!database_.isPrimaryKeyForeignKey(build_attr, probe_attr)) {
    return node;
  }

  if (filter_predicate == nullptr) {
    return probe;
  }

  std::unordered_map<ExprId, ExpressionPtr> substitution_map;
  substitution_map.emplace(equi_join->build_attributes().front()->id(),
                           equi_join->probe_attributes().front());
  UpdateExpression updater(substitution_map);
  const ExpressionPtr expression = updater.apply(filter_predicate);
  CHECK(SomePredicate::MatchesWithConditionalCast(expression, &filter_predicate));

  return Selection::Create(probe,
                           CastSharedPtrVector<Scalar>(probe->getOutputAttributes()),
                           filter_predicate);
}

}  // namespace optimizer
}  // namespace project
