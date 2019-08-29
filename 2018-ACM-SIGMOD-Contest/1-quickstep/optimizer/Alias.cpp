#include "optimizer/Alias.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/PatternMatcher.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

ExpressionPtr Alias::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  return Create(new_children[0], id_, name_);
}

std::vector<AttributeReferencePtr> Alias::getReferencedAttributes() const {
  return expression_->getReferencedAttributes();
}

AttributeReferencePtr Alias::getAttribute() const {
  return AttributeReference::Create(id_, expression_->getValueType(), name_);
}

std::string Alias::toShortString() const {
  const std::string expr_name = expression_->toShortString();
  if (expr_name == name_) {
    return name_;
  } else {
    return name_ + "<-" + expr_name;
  }
}

::project::Scalar* Alias::concretize(
    const std::unordered_map<ExprId, const Attribute*> &substitution_map) const {
  ScalarPtr scalar;
  CHECK(SomeScalar::MatchesWithConditionalCast(expression_, &scalar));
  return scalar->concretize(substitution_map);
}

void Alias::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("expression");
  non_container_child_fields->emplace_back(expression_);

  inline_field_names->emplace_back("name");
  inline_field_values->emplace_back(name_);

  inline_field_names->emplace_back("id");
  inline_field_values->emplace_back(std::to_string(id_));
}

}  // namespace optimizer
}  // namespace project
