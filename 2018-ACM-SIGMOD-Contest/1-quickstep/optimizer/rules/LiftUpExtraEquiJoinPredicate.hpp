#ifndef PROJECT_OPTIMIZER_RULES_LIFT_UP_EXTRA_EQUI_JOIN_PREDICATE_HPP_
#define PROJECT_OPTIMIZER_RULES_LIFT_UP_EXTRA_EQUI_JOIN_PREDICATE_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class LiftUpExtraEquiJoinPredicate : public BottomUpRule<Plan> {
 public:
  LiftUpExtraEquiJoinPredicate() {}

  std::string getName() const override {
    return "LiftUpExtraEquiJoinPredicate";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(LiftUpExtraEquiJoinPredicate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_LIFT_UP_EXTRA_EQUI_JOIN_PREDICATE_HPP_
