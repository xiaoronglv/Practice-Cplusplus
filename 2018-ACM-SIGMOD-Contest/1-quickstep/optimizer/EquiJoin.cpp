#include "optimizer/EquiJoin.hpp"

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

std::string EquiJoin::getName() const {
  switch (join_type_) {
    case kHashInnerJoin:
      return "HashJoin";
    case kLeftSemiJoin:
      return "HashLeftSemiJoin";
    case kPrimaryKeyIndexJoin:
      return "PKIndexJoin";
    case kSortMergeJoin:
      return "SortMergeJoin";
    default:
      break;
  }
  return "UnknownJoin";
}

std::vector<AttributeReferencePtr> EquiJoin::getOutputAttributes() const {
  return ToRefVector(project_expressions_);
}

std::vector<AttributeReferencePtr> EquiJoin::getReferencedAttributes() const {
  auto referenced_attributes = GetReferencedAttributes(project_expressions_);
  InsertAll(probe_attributes_, &referenced_attributes);
  InsertAll(build_attributes_, &referenced_attributes);
  if (probe_filter_predicate_ != nullptr) {
    InsertAll(probe_filter_predicate_->getReferencedAttributes(),
              &referenced_attributes);
  }
  if (build_filter_predicate_ != nullptr) {
    InsertAll(build_filter_predicate_->getReferencedAttributes(),
              &referenced_attributes);
  }
  return referenced_attributes;
}

PlanPtr EquiJoin::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_EQ(children().size(), new_children.size());
  return Create(new_children[0],
                new_children[1],
                probe_attributes_,
                build_attributes_,
                project_expressions_,
                probe_filter_predicate_,
                build_filter_predicate_,
                join_type_);
}

bool EquiJoin::maybeCopyWithPrunedAttributes(
    const UnorderedAttributeSet &referenced_attributes, PlanPtr *output) const {
  const std::vector<ScalarPtr> new_project_expressions =
      GetReferencedExpressions(referenced_attributes, project_expressions_);
  if (new_project_expressions.size() != project_expressions_.size()) {
    *output = Create(probe_,
                     build_,
                     probe_attributes_,
                     build_attributes_,
                     new_project_expressions,
                     probe_filter_predicate_,
                     build_filter_predicate_,
                     join_type_);
    return true;
  }
  return false;
}

void EquiJoin::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("probe");
  non_container_child_fields->emplace_back(probe_);
  non_container_child_field_names->emplace_back("build");
  non_container_child_fields->emplace_back(build_);

  container_child_field_names->emplace_back("probe_attributes");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(probe_attributes_));
  container_child_field_names->emplace_back("build_attributes");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(build_attributes_));

  if (probe_filter_predicate_ != nullptr) {
    non_container_child_field_names->emplace_back("probe_filter_predicate");
    non_container_child_fields->emplace_back(probe_filter_predicate_);
  }
  if (build_filter_predicate_ != nullptr) {
    non_container_child_field_names->emplace_back("build_filter_predicate");
    non_container_child_fields->emplace_back(build_filter_predicate_);
  }

  container_child_field_names->emplace_back("project_expressions");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(project_expressions_));
}

}  // namespace optimizer
}  // namespace project
