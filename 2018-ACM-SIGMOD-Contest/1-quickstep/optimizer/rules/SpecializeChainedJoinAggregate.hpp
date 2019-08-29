#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_CHAINED_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_CHAINED_JOIN_AGGREGATE_HPP_

#include <string>
#include <unordered_set>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializeChainedJoinAggregate : public BottomUpRule<Plan> {
 public:
  SpecializeChainedJoinAggregate() {}

  std::string getName() const override {
    return "SpecializeChainedJoinAggregate";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  DISALLOW_COPY_AND_ASSIGN(SpecializeChainedJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_CHAINED_JOIN_AGGREGATE_HPP_
