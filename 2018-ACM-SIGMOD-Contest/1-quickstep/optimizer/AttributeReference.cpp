#include "optimizer/AttributeReference.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/Expression.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/PatternMatcher.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

ExpressionPtr AttributeReference::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  return Create(id_, type_, name_);
}

std::vector<AttributeReferencePtr> AttributeReference::getReferencedAttributes() const {
  return { Create(id_, type_, name_) };
}

AttributeReferencePtr AttributeReference::getAttribute() const {
  return AttributeReference::Create(id_, type_, name_);
}

::project::Scalar* AttributeReference::concretize(
    const std::unordered_map<ExprId, const Attribute*> &substitution_map) const {
  const auto it = substitution_map.find(id_);
  DCHECK(it != substitution_map.end());
  return new ScalarAttribute(it->second);
}

void AttributeReference::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  inline_field_names->emplace_back("name");
  inline_field_values->emplace_back("\"" + name_ + "\"");

  inline_field_names->emplace_back("id");
  inline_field_values->emplace_back(std::to_string(id_));

  inline_field_names->emplace_back("type");
  inline_field_values->emplace_back(type_.getName());
}

}  // namespace optimizer
}  // namespace project
