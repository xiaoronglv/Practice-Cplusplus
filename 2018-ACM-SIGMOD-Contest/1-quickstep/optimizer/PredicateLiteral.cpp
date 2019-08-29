#include "optimizer/PredicateLiteral.hpp"

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

#include "operators/expressions/TruePredicate.hpp"
#include "optimizer/AttributeReference.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

ExpressionPtr PredicateLiteral::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  return Create(value_);
}

std::vector<AttributeReferencePtr> PredicateLiteral::getReferencedAttributes() const {
  return {};
}

Range PredicateLiteral::reduceRange(const ExprId expr_id, const Range &input) const {
  return value_ ? input : Range();
}

::project::Predicate* PredicateLiteral::concretize(
    const std::unordered_map<ExprId, const Attribute*> &substitution_map) const {
  if (!value_) {
    LOG(FATAL) << "Unexpected value in PredicateLiteral::concretize(): false";
  }
  return new TruePredicate();
}

void PredicateLiteral::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  inline_field_names->emplace_back("value");
  inline_field_values->emplace_back(value_ ? "true" : "false");
}

}  // namespace optimizer
}  // namespace project
