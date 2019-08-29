#include "optimizer/ScalarLiteral.hpp"

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

#include "operators/expressions/ScalarLiteral.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/Scalar.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

ExpressionPtr ScalarLiteral::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  return ScalarLiteral::Create(value_, value_type_, expr_id_);
}

std::vector<AttributeReferencePtr> ScalarLiteral::getReferencedAttributes() const {
  return {};
}

AttributeReferencePtr ScalarLiteral::getAttribute() const {
  return AttributeReference::Create(expr_id_, value_type_, "");
}

std::string ScalarLiteral::toShortString() const {
  return std::to_string(value_);
}

::project::Scalar* ScalarLiteral::concretize(
    const std::unordered_map<ExprId, const Attribute*> &substitution_map) const {
  return new ::project::ScalarLiteral(value_, value_type_);
}

void ScalarLiteral::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  inline_field_names->emplace_back("value");
  inline_field_values->emplace_back(std::to_string(value_));

  inline_field_names->emplace_back("value_type");
  inline_field_values->emplace_back(value_type_.getName());
}

}  // namespace optimizer
}  // namespace project
