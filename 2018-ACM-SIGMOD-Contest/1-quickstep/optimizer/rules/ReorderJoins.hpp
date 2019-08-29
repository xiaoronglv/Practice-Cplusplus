#ifndef PROJECT_OPTIMIZER_RULES_REORDER_JOINS_HPP_
#define PROJECT_OPTIMIZER_RULES_REORDER_JOINS_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "optimizer/ExprId.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/Rule.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ReorderJoins : public Rule<Plan> {
 public:
  explicit ReorderJoins(const Database &database)
      : database_(database) {}

  std::string getName() const override {
    return "ReorderJoins";
  }

  PlanPtr apply(const PlanPtr &input) override;

 private:
  struct JoinGroupInfo {
    std::vector<PlanPtr> tables;
    std::vector<std::pair<ExprId, ExprId>> join_attribute_pairs;
  };

  PlanPtr applyInternal(const PlanPtr &input,
                        JoinGroupInfo *parent_join_group) const;

  PlanPtr generatePlan(const JoinGroupInfo &join_group,
                       const std::vector<ScalarPtr> &project_expressions) const;

  const Database &database_;
  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(ReorderJoins);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_REORDER_JOINS_HPP_
