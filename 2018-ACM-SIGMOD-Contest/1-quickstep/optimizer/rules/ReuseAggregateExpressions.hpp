#ifndef PROJECT_OPTIMIZER_RULES_REUSE_AGGREGATE_EXPRESSIONS_HPP_
#define PROJECT_OPTIMIZER_RULES_REUSE_AGGREGATE_EXPRESSIONS_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ReuseAggregateExpressions : public BottomUpRule<Plan> {
 public:
  ReuseAggregateExpressions() {}

  std::string getName() const override {
    return "ReuseAggregateExpressions";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  DISALLOW_COPY_AND_ASSIGN(ReuseAggregateExpressions);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_REUSE_AGGREGATE_EXPRESSIONS_HPP_
