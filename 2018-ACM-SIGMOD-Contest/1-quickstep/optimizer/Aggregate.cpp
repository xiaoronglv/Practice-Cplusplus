#include "optimizer/Aggregate.hpp"

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

std::vector<AttributeReferencePtr> Aggregate::getOutputAttributes() const {
  return ToRefVector(aggregate_expressions_);
}

std::vector<AttributeReferencePtr> Aggregate::getReferencedAttributes() const {
  std::vector<AttributeReferencePtr> referenced_attributes;
  for (const auto &expr : aggregate_expressions_) {
    InsertAll(expr->getReferencedAttributes(), &referenced_attributes);
  }
  if (filter_predicate_ != nullptr) {
    InsertAll(filter_predicate_->getReferencedAttributes(), &referenced_attributes);
  }
  return referenced_attributes;
}

PlanPtr Aggregate::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0], aggregate_expressions_, filter_predicate_);
}

bool Aggregate::maybeCopyWithPrunedAttributes(
    const UnorderedAttributeSet &referenced_attributes, PlanPtr *output) const {
  const std::vector<ScalarPtr> new_aggregate_expressions =
      GetReferencedExpressions(referenced_attributes, aggregate_expressions_);
  if (new_aggregate_expressions.size() != aggregate_expressions_.size()) {
    *output = Create(input_, new_aggregate_expressions, filter_predicate_);
    return true;
  }
  return false;
}

void Aggregate::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("input");
  non_container_child_fields->emplace_back(input_);

  container_child_field_names->emplace_back("aggregate_expressions");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(aggregate_expressions_));

  if (filter_predicate_ != nullptr) {
    non_container_child_field_names->emplace_back("filter_predicate");
    non_container_child_fields->emplace_back(filter_predicate_);
  }
}


}  // namespace optimizer
}  // namespace project
