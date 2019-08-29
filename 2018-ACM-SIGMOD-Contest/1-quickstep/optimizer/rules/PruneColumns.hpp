#ifndef PROJECT_OPTIMIZER_RULES_PRUNE_COLUMNS_HPP_
#define PROJECT_OPTIMIZER_RULES_PRUNE_COLUMNS_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/TopDownRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class PruneColumns : public TopDownRule<Plan> {
 public:
  PruneColumns() {}

  std::string getName() const override {
    return "PruneColumns";
  }

 protected:
  PlanPtr applyToNode(const PlanPtr &node) override;

 private:
  DISALLOW_COPY_AND_ASSIGN(PruneColumns);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_PRUNE_COLUMNS_HPP_
