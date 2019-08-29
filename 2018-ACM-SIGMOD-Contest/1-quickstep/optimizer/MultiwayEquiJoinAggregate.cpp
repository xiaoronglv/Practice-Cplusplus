#include "optimizer/MultiwayEquiJoinAggregate.hpp"

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

std::vector<AttributeReferencePtr> MultiwayEquiJoinAggregate::getOutputAttributes() const {
  return ToRefVector(aggregate_expressions_);
}

std::vector<AttributeReferencePtr> MultiwayEquiJoinAggregate::getReferencedAttributes() const {
  auto referenced_attributes = GetReferencedAttributes(aggregate_expressions_);
  InsertAll(join_attributes_, &referenced_attributes);
  for (const auto &predicate : filter_predicates_) {
    if (predicate != nullptr) {
      InsertAll(predicate->getReferencedAttributes(), &referenced_attributes);
    }
  }
  return referenced_attributes;
}

PlanPtr MultiwayEquiJoinAggregate::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children,
                join_attributes_,
                aggregate_expressions_,
                filter_predicates_);
}

bool MultiwayEquiJoinAggregate::maybeCopyWithPrunedAttributes(
    const UnorderedAttributeSet &referenced_attributes,
    PlanPtr *output) const {
  return false;
}

void MultiwayEquiJoinAggregate::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  container_child_field_names->emplace_back("inputs");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(inputs_));

  container_child_field_names->emplace_back("join_attributes");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(join_attributes_));

  container_child_field_names->emplace_back("aggregate_expressions");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(aggregate_expressions_));

  container_child_field_names->emplace_back("filter_predicates");
  container_child_fields->emplace_back();
  for (const auto &predicate : filter_predicates_) {
    if (predicate != nullptr) {
      container_child_fields->back().emplace_back(predicate);
    }
  }
}

}  // namespace optimizer
}  // namespace project
