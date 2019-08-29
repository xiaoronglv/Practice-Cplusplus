#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_FOREIGN_KEY_INDEX_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_FOREIGN_KEY_INDEX_JOIN_AGGREGATE_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializeForeignKeyIndexJoinAggregate : public BottomUpRule<Plan> {
 public:
  SpecializeForeignKeyIndexJoinAggregate() {}

  std::string getName() const override {
    return "SpecializeForeignKeyIndexJoinAggregate";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(SpecializeForeignKeyIndexJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_FOREIGN_KEY_INDEX_JOIN_AGGREGATE_HPP_
