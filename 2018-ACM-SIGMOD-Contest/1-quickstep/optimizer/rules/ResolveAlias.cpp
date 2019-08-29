#include "optimizer/rules/ResolveAlias.hpp"

#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

namespace {

class ResolveAliasInExpression : public BottomUpRule<Expression> {
 public:
  explicit ResolveAliasInExpression(std::unordered_map<ExprId, ScalarPtr> *substitution_map)
      : substitution_map_(substitution_map) {}

  std::string getName() const override {
    return "ResolveAliasInExpression";
  }

 protected:
  ExpressionPtr applyToNode(const ExpressionPtr &node) override {
    ScalarPtr scalar;
    if (!SomeScalar::MatchesWithConditionalCast(node, &scalar)) {
      return node;
    }

    AliasPtr alias;
    if (!SomeAlias::MatchesWithConditionalCast(scalar, &alias)) {
      const auto it = substitution_map_->find(scalar->getAttribute()->id());
      if (it == substitution_map_->end()) {
        return node;
      } else {
        return it->second;
      }
    }

    ScalarPtr child;
    if (!SomeScalar::MatchesWithConditionalCast(alias->expression(), &child)) {
      return node;
    }

    (*substitution_map_)[alias->id()] = child;
    return child;
  }

 private:
  std::unordered_map<ExprId, ScalarPtr> *substitution_map_;

  DISALLOW_COPY_AND_ASSIGN(ResolveAliasInExpression);
};

inline void ResolveAliasInExpressions(
    std::vector<ScalarPtr> *expressions,
    PredicatePtr *predicate,
    std::unordered_map<ExprId, ScalarPtr> *substitution_map) {
  ResolveAliasInExpression resolver(substitution_map);

  if (expressions != nullptr) {
    for (ScalarPtr &scalar : *expressions) {
      scalar = std::static_pointer_cast<const Scalar>(resolver.apply(scalar));
    }
  }

  if (predicate != nullptr && *predicate != nullptr) {
    *predicate = std::static_pointer_cast<const Predicate>(resolver.apply(*predicate));
  }
}

}  // namespace

PlanPtr ResolveAlias::applyToNode(const PlanPtr &node) {
  switch (node->getPlanType()) {
    case PlanType::kAggregate: {
      const AggregatePtr &aggregate =
          std::static_pointer_cast<const Aggregate>(node);
      std::vector<ScalarPtr> aggregate_expressions = aggregate->aggregate_expressions();
      PredicatePtr filter_predicate = aggregate->filter_predicate();

      ResolveAliasInExpressions(&aggregate_expressions,
                                &filter_predicate,
                                &substitution_map_);

      return Aggregate::Create(aggregate->input(),
                               aggregate_expressions,
                               filter_predicate);
    }
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(node);
      std::vector<ScalarPtr> project_expressions = selection->project_expressions();
      PredicatePtr filter_predicate = selection->filter_predicate();

      ResolveAliasInExpressions(&project_expressions,
                                &filter_predicate,
                                &substitution_map_);

      return Selection::Create(selection->input(),
                               project_expressions,
                               filter_predicate,
                               selection->selection_type());
    }
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(node);
      std::vector<ScalarPtr> project_expressions = equi_join->project_expressions();

      DCHECK(equi_join->probe_filter_predicate() == nullptr);
      DCHECK(equi_join->build_filter_predicate() == nullptr);

      ResolveAliasInExpressions(&project_expressions,
                                nullptr /* predicate */,
                                &substitution_map_);

      return EquiJoin::Create(equi_join->probe(),
                              equi_join->build(),
                              equi_join->probe_attributes(),
                              equi_join->build_attributes(),
                              project_expressions,
                              equi_join->probe_filter_predicate(),
                              equi_join->build_filter_predicate(),
                              equi_join->join_type());
    }
    default:
      break;
  }
  return node;
}

}  // namespace optimizer
}  // namespace project
