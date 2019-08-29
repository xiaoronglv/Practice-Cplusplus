#include "optimizer/rules/ReuseAggregateExpressions.hpp"

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AggregateFunction.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr ReuseAggregateExpressions::applyToNode(const PlanPtr &node) {
  AggregatePtr aggregate;
  if (!SomeAggregate::MatchesWithConditionalCast(node, &aggregate)) {
    return node;
  }

  std::unordered_map<ScalarPtr, std::vector<std::size_t>,
                     ScalarHash, ScalarEqual> locations;
  std::unordered_map<ExprId, ExprId> mapping;

  const auto &aggregate_expressions = aggregate->aggregate_expressions();
  const std::size_t num_aggregates = aggregate_expressions.size();

  for (std::size_t i = 0; i < num_aggregates; ++i) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(aggregate_expressions[i], &alias));

    AggregateFunctionPtr func;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(), &func));

    locations[func->argument()].emplace_back(i);
    mapping.emplace(func->argument()->getAttribute()->id(), alias->id());
  }

  if (locations.size() == num_aggregates) {
    return node;
  }

  std::vector<ScalarPtr> new_aggregate_expressions;
  std::vector<ScalarPtr> project_expressions(num_aggregates);
  for (const auto &it : locations) {
    const AggregateFunctionPtr new_func =
        AggregateFunction::Create(AggregateFunctionType::kSum, it.first);

    const AliasPtr new_alias =
        Alias::Create(new_func,
                      mapping.at(it.first->getAttribute()->id()),
                      new_func->toShortString());

    new_aggregate_expressions.emplace_back(new_alias);

    const AttributeReferencePtr attribute = new_alias->getAttribute();

    for (const std::size_t i : it.second) {
      AliasPtr alias;
      CHECK(SomeAlias::MatchesWithConditionalCast(aggregate_expressions[i], &alias));

      project_expressions[i] = Alias::Create(attribute, alias->id(), alias->name());
    }
  }

  const AggregatePtr new_aggregate =
      Aggregate::Create(aggregate->input(),
                        new_aggregate_expressions,
                        aggregate->filter_predicate());

  return Selection::Create(new_aggregate, project_expressions, nullptr);
}

}  // namespace optimizer
}  // namespace project
