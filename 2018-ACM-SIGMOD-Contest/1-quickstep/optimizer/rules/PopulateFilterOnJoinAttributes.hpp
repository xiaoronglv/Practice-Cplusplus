#ifndef PROJECT_OPTIMIZER_RULES_POPULATE_FILTER_ON_JOIN_ATTRIBUTES_HPP_
#define PROJECT_OPTIMIZER_RULES_POPULATE_FILTER_ON_JOIN_ATTRIBUTES_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/TopDownRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class PopulateFilterOnJoinAttributes : public TopDownRule<Plan> {
 public:
  PopulateFilterOnJoinAttributes() {}

  std::string getName() const override {
    return "PopulateFilterOnJoinAttributes";
  }

 protected:
  PlanPtr applyToNode(const PlanPtr &node) override;

 private:
  DISALLOW_COPY_AND_ASSIGN(PopulateFilterOnJoinAttributes);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_POPULATE_FILTER_ON_JOIN_ATTRIBUTES_HPP_
