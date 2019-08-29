#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_SELECTION_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_SELECTION_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializeSelection : public BottomUpRule<Plan> {
 public:
  SpecializeSelection() {}

  std::string getName() const override {
    return "SpecializeSelection";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(SpecializeSelection);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_SELECTION_HPP_
