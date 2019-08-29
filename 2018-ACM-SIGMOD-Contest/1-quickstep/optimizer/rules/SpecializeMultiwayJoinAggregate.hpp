#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_MULTIWAY_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_MULTIWAY_JOIN_AGGREGATE_HPP_

#include <string>
#include <unordered_set>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/MultiwayEquiJoinAggregate.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializeMultiwayJoinAggregate : public BottomUpRule<Plan> {
 public:
  SpecializeMultiwayJoinAggregate() {}

  std::string getName() const override {
    return "SpecializeMultiwayJoinAggregate";
  }

 private:
  struct JoinInfo {
    JoinInfo() : has_basic_join(false) {
      plans.reserve(4u);
      join_attributes.reserve(4u);
      filter_predicates.reserve(4u);
      join_attribute_class.reserve(4u);
    }

    const std::vector<ScalarPtr> *aggregate_expressions;
    std::vector<PlanPtr> plans;
    std::vector<AttributeReferencePtr> join_attributes;
    std::vector<PredicatePtr> filter_predicates;
    std::unordered_set<ExprId> join_attribute_class;
    bool has_basic_join;
  };

  void collect(const PlanPtr &plan, JoinInfo *info);
  void collectAggregate(const AggregatePtr &aggregate, JoinInfo *info);
  void collectEquiJoinAggregate(
      const EquiJoinAggregatePtr &equi_join_aggregate, JoinInfo *info);

  bool collectEquiJoin(PlanPtr probe,
                       PlanPtr build,
                       PredicatePtr probe_filter_predicate,
                       PredicatePtr build_filter_predicate,
                       const std::vector<AttributeReferencePtr> &probe_attributes,
                       const std::vector<AttributeReferencePtr> &build_attributes,
                       const bool is_basic_join,
                       JoinInfo *info);

  PlanPtr rewriteAggregateExpressions(
      const MultiwayEquiJoinAggregatePtr &multiway_join_aggr) const;

  PlanPtr applyToNode(const PlanPtr &node) override;

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(SpecializeMultiwayJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_MULTIWAY_JOIN_AGGREGATE_HPP_

