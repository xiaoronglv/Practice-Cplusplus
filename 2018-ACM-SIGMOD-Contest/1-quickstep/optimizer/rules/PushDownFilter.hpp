#ifndef PROJECT_OPTIMIZER_RULES_PUSH_DOWN_FILTER_HPP_
#define PROJECT_OPTIMIZER_RULES_PUSH_DOWN_FILTER_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/TopDownRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class PushDownFilter : public TopDownRule<Plan> {
 public:
  PushDownFilter() {}

  std::string getName() const override {
    return "PushDownFilter";
  }

 protected:
  PlanPtr applyToNode(const PlanPtr &node) override;

 private:
  DISALLOW_COPY_AND_ASSIGN(PushDownFilter);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_PUSH_DOWN_FILTER_HPP_
