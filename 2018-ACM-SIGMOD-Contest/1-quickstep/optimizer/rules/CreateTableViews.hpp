#ifndef PROJECT_OPTIMIZER_RULES_CREATE_TABLE_VIEWS_HPP_
#define PROJECT_OPTIMIZER_RULES_CREATE_TABLE_VIEWS_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {

class RelationStatistics;

namespace optimizer {

class CreateTableViews : public BottomUpRule<Plan> {
 public:
  CreateTableViews() {}

  std::string getName() const override {
    return "CreateTableViews";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;
  PlanPtr tryCreate(const PredicatePtr &predicate,
                    const PlanPtr &node,
                    const RelationStatistics &stat) const;

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(CreateTableViews);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_CREATE_TABLE_VIEWS_HPP_
