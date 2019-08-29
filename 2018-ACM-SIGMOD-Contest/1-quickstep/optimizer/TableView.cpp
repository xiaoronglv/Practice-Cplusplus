#include "optimizer/TableView.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Plan.hpp"
#include "storage/AttributeStatistics.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> TableView::getOutputAttributes() const {
  return input_->getOutputAttributes();
}

std::vector<AttributeReferencePtr> TableView::getReferencedAttributes() const {
  return getOutputAttributes();
}

PlanPtr TableView::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0], comparison_, sort_order_);
}

bool TableView::maybeCopyWithPrunedAttributes(
    const UnorderedAttributeSet &referenced_attributes,
    PlanPtr *output) const {
  return false;
}

void TableView::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("input");
  non_container_child_fields->emplace_back(input_);

  non_container_child_field_names->emplace_back("comparison");
  non_container_child_fields->emplace_back(comparison_);

  inline_field_names->emplace_back("sort_order");
  inline_field_values->emplace_back(GetSortOrderString(sort_order_));
}

}  // namespace optimizer
}  // namespace project
