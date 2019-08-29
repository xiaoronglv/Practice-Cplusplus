#ifndef PROJECT_OPTIMIZER_RULES_COLLAPSE_SELECTION_HPP_
#define PROJECT_OPTIMIZER_RULES_COLLAPSE_SELECTION_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class CollapseSelection : public Rule<Plan> {
 public:
  CollapseSelection() {}

  std::string getName() const override {
    return "CollapseSelection";
  }

  PlanPtr apply(const PlanPtr &input) override;

 private:
  PlanPtr applyInternal(const PlanPtr &input, const bool is_output_fixed);
  PlanPtr applyToNode(const PlanPtr &node, const bool is_output_fixed);

  DISALLOW_COPY_AND_ASSIGN(CollapseSelection);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_COLLAPSE_SELECTION_HPP_
