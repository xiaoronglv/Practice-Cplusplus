#include "optimizer/Filter.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> Filter::getOutputAttributes() const {
  return input_->getOutputAttributes();
}

std::vector<AttributeReferencePtr> Filter::getReferencedAttributes() const {
  return getOutputAttributes();
}

PlanPtr Filter::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Filter::Create(new_children[0], filter_predicate_);
}

void Filter::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("input");
  non_container_child_fields->emplace_back(input_);

  non_container_child_field_names->emplace_back("filter_predicate");
  non_container_child_fields->emplace_back(filter_predicate_);
}

}  // namespace optimizer
}  // namespace project
