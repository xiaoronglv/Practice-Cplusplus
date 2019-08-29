#ifndef PROJECT_OPTIMIZER_TOP_LEVEL_PLAN_HPP_
#define PROJECT_OPTIMIZER_TOP_LEVEL_PLAN_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

class TopLevelPlan;
typedef std::shared_ptr<const TopLevelPlan> TopLevelPlanPtr;

class TopLevelPlan : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kTopLevelPlan;
  }

  std::string getName() const override {
    return "TopLevelPlan";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  const PlanPtr& plan() const {
    return plan_;
  }

  static TopLevelPlanPtr Create(const PlanPtr &plan) {
    return TopLevelPlanPtr(new TopLevelPlan(plan));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override;

 private:
  explicit TopLevelPlan(const PlanPtr &plan)
      : plan_(plan) {
    addChild(plan_);
  }

  const PlanPtr plan_;

  DISALLOW_COPY_AND_ASSIGN(TopLevelPlan);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_TOP_LEVEL_PLAN_HPP_
