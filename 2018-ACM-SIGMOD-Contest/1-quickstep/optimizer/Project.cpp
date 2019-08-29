#include "optimizer/Project.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> Project::getOutputAttributes() const {
  return project_attributes_;
}

std::vector<AttributeReferencePtr> Project::getReferencedAttributes() const {
  return getOutputAttributes();
}

PlanPtr Project::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0], project_attributes_);
}

void Project::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("input");
  non_container_child_fields->emplace_back(input_);

  container_child_field_names->emplace_back("project_attributes");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(project_attributes_));
}

}  // namespace optimizer
}  // namespace project
