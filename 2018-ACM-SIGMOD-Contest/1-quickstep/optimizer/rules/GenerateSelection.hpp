#ifndef PROJECT_OPTIMIZER_RULES_GENERATE_SELECTION_HPP_
#define PROJECT_OPTIMIZER_RULES_GENERATE_SELECTION_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/TopDownRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class GenerateSelection : public TopDownRule<Plan> {
 public:
  GenerateSelection() {}

  std::string getName() const override {
    return "GenerateSelection";
  }

 protected:
  PlanPtr applyToNode(const PlanPtr &node) override;

 private:
  DISALLOW_COPY_AND_ASSIGN(GenerateSelection);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_GENERATE_SELECTION_HPP_
