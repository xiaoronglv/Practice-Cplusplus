#ifndef PROJECT_OPTIMIZER_RULES_REDUCE_PREDICATE_HPP_
#define PROJECT_OPTIMIZER_RULES_REDUCE_PREDICATE_HPP_

#include <string>

#include "optimizer/OptimizerContext.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ReducePredicate : public BottomUpRule<Plan> {
 public:
  explicit ReducePredicate(OptimizerContext *optimizer_context)
      : optimizer_context_(optimizer_context) {}

  std::string getName() const override {
    return "ReducePredicate";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  PredicatePtr reduce(const PredicatePtr &filter_predicate) const;

  OptimizerContext *optimizer_context_;

  DISALLOW_COPY_AND_ASSIGN(ReducePredicate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_REDUCE_PREDICATE_HPP_
