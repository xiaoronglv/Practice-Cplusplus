#include "optimizer/MultiwayCartesianJoin.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::vector<AttributeReferencePtr> MultiwayCartesianJoin::getOutputAttributes() const {
  std::vector<AttributeReferencePtr> output_attributes;
  for (const auto &operand : operands_) {
    InsertAll(operand->getOutputAttributes(), &output_attributes);
  }
  return output_attributes;
}

std::vector<AttributeReferencePtr> MultiwayCartesianJoin::getReferencedAttributes() const {
  return getOutputAttributes();
}

PlanPtr MultiwayCartesianJoin::copyWithNewChildren(
    const std::vector<PlanPtr> &new_children) const {
  DCHECK_GT(new_children.size(), 1u);
  return MultiwayCartesianJoinPtr(new MultiwayCartesianJoin(new_children));
}

void MultiwayCartesianJoin::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  container_child_field_names->emplace_back("");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(operands_));
}

}  // namespace optimizer
}  // namespace project
