#include "optimizer/TopLevelPlan.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> TopLevelPlan::getOutputAttributes() const {
  return plan_->getOutputAttributes();
}

std::vector<AttributeReferencePtr> TopLevelPlan::getReferencedAttributes() const {
  return getOutputAttributes();
}

PlanPtr TopLevelPlan::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0]);
}

void TopLevelPlan::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("plan");
  non_container_child_fields->emplace_back(plan_);
}

}  // namespace optimizer
}  // namespace project
