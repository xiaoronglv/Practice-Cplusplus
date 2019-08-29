#ifndef PROJECT_OPTIMIZER_RULES_REDUCE_SEMI_JOIN_HPP_
#define PROJECT_OPTIMIZER_RULES_REDUCE_SEMI_JOIN_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ReduceSemiJoin : public BottomUpRule<Plan> {
 public:
  explicit ReduceSemiJoin(const Database &database)
      : database_(database) {}

  std::string getName() const override {
    return "ReduceSemiJoin";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  const Database &database_;
  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(ReduceSemiJoin);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_REDUCE_SEMI_JOIN_HPP_
