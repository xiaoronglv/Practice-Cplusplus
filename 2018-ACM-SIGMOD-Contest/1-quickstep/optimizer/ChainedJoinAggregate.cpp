#include "optimizer/ChainedJoinAggregate.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Plan.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> ChainedJoinAggregate::getOutputAttributes() const {
  return ToRefVector(aggregate_expressions_);
}

std::vector<AttributeReferencePtr> ChainedJoinAggregate::getReferencedAttributes() const {
  auto referenced_attributes = GetReferencedAttributes(aggregate_expressions_);
  for (const auto &pair : join_attribute_pairs_) {
    referenced_attributes.emplace_back(pair.first);
    referenced_attributes.emplace_back(pair.second);
  }
  for (const auto &predicate : filter_predicates_) {
    if (predicate != nullptr) {
      InsertAll(predicate->getReferencedAttributes(), &referenced_attributes);
    }
  }
  return referenced_attributes;
}

PlanPtr ChainedJoinAggregate::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children,
                join_attribute_pairs_,
                aggregate_expressions_,
                filter_predicates_);
}

bool ChainedJoinAggregate::maybeCopyWithPrunedAttributes(
    const UnorderedAttributeSet &referenced_attributes,
    PlanPtr *output) const {
  return false;
}

void ChainedJoinAggregate::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  container_child_field_names->emplace_back("inputs");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(inputs_));

  container_child_field_names->emplace_back("probe_attributes");
  container_child_fields->emplace_back();
  for (const auto &pair : join_attribute_pairs_) {
    container_child_fields->back().emplace_back(pair.second);
  }

  container_child_field_names->emplace_back("build_attributes");
  container_child_fields->emplace_back();
  for (const auto &pair : join_attribute_pairs_) {
    container_child_fields->back().emplace_back(pair.first);
  }

  container_child_field_names->emplace_back("aggregate_expressions");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(aggregate_expressions_));

  container_child_field_names->emplace_back("filter_predicates");
  container_child_fields->emplace_back();
  for (const auto &predicate : filter_predicates_) {
    if (predicate) {
      container_child_fields->back().emplace_back(predicate);
    }
  }
}

}  // namespace optimizer
}  // namespace project

