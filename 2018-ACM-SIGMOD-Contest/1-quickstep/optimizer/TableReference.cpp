#include "optimizer/TableReference.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/OptimizerContext.hpp"
#include "storage/Relation.hpp"
#include "utility/MemoryUtil.hpp"
#include "utility/StringUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> TableReference::getOutputAttributes() const {
  return attribute_list_;
}

std::vector<AttributeReferencePtr> TableReference::getReferencedAttributes() const {
  return {};
}

PlanPtr TableReference::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return TableReferencePtr(new TableReference(relation_, attribute_list_));
}

void TableReference::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  inline_field_names->emplace_back("relation");
  inline_field_values->emplace_back(relation_.getName());

  /*
  std::vector<ExprId> attrs;
  for (const auto &attr : attribute_list_) {
    attrs.emplace_back(attr->id());
  }
  inline_field_names->emplace_back("attribute_list");
  inline_field_values->emplace_back(ConcatToString(attrs, ","));
  */

  container_child_field_names->push_back("");
  container_child_fields->push_back(CastSharedPtrVector<OptimizerTreeBase>(attribute_list_));
}

std::vector<AttributeReferencePtr> TableReference::CreateAttributeList(
    const Relation &relation, OptimizerContext *optimizer_context) {
  const std::size_t num_attributes = relation.getNumAttributes();
  std::vector<AttributeReferencePtr> attributes(num_attributes);
  for (std::size_t i = 0; i < num_attributes; ++i) {
    attributes[i] = AttributeReference::Create(
        optimizer_context->nextExprId(),
        relation.getAttributeType(i),
        relation.getName() + "." + relation.getAttributeName(i));
  }
  return attributes;
}

}  // namespace optimizer
}  // namespace project
