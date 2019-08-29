#include "optimizer/Selection.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::string Selection::getName() const {
  switch (selection_type_) {
    case kBasic:
      return "Selection";
    case kIndexScan:
      return "IndexScan";
    default:
      break;
  }
  return "UnknownSelection";
}

std::vector<AttributeReferencePtr> Selection::getOutputAttributes() const {
  return ToRefVector(project_expressions_);
}

std::vector<AttributeReferencePtr> Selection::getReferencedAttributes() const {
  auto referenced_attributes = GetReferencedAttributes(project_expressions_);
  if (filter_predicate_ != nullptr) {
    InsertAll(filter_predicate_->getReferencedAttributes(), &referenced_attributes);
  }
  return referenced_attributes;
}

PlanPtr Selection::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0],
                project_expressions_,
                filter_predicate_,
                selection_type_);
}

bool Selection::maybeCopyWithPrunedAttributes(
    const UnorderedAttributeSet &referenced_attributes,
    PlanPtr *output) const {
  const std::vector<ScalarPtr> new_project_expressions =
      GetReferencedExpressions(referenced_attributes, project_expressions_);
  if (new_project_expressions.size() != project_expressions_.size()) {
    *output = Create(input(),
                     new_project_expressions,
                     filter_predicate_,
                     selection_type_);
    return true;
  }
  return false;
}

void Selection::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("input");
  non_container_child_fields->emplace_back(input_);

  container_child_field_names->emplace_back("project_expressions");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(project_expressions_));

  if (filter_predicate_ != nullptr) {
    non_container_child_field_names->emplace_back("filter_predicate");
    non_container_child_fields->emplace_back(filter_predicate_);
  }
}

}  // namespace optimizer
}  // namespace project
