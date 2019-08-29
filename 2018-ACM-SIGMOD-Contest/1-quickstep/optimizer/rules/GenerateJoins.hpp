#ifndef PROJECT_OPTIMIZER_RULES_GENERATE_JOINS_HPP_
#define PROJECT_OPTIMIZER_RULES_GENERATE_JOINS_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/TopDownRule.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class GenerateJoins : public TopDownRule<Plan> {
 public:
  explicit GenerateJoins(const Database &database)
      : database_(database) {}

  std::string getName() const override {
    return "GenerateJoins";
  }

 protected:
  PlanPtr applyToNode(const PlanPtr &node) override;

 private:
  const Database &database_;
  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(GenerateJoins);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_GENERATE_JOINS_HPP_
