#include "optimizer/rules/PruneColumns.hpp"

#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"

namespace project {
namespace optimizer {

PlanPtr PruneColumns::applyToNode(const PlanPtr &node) {
  if (node->getNumChildren() == 0 ||
      node->getPlanType() == PlanType::kTopLevelPlan) {
    return node;
  }

  const auto referenced_attributes = node->getReferencedAttributes();
  const UnorderedAttributeSet referenced_attributes_set(
      referenced_attributes.begin(), referenced_attributes.end());

  std::vector<PlanPtr> new_children;
  for (const auto &child : node->children()) {
    PlanPtr new_child;
    if (child->maybeCopyWithPrunedAttributes(referenced_attributes_set, &new_child)) {
      new_children.emplace_back(new_child);
    } else {
      new_children.emplace_back(child);
    }
  }

  if (new_children == node->children()) {
    return node;
  } else {
    return node->copyWithNewChildren(new_children);
  }
}

}  // namespace optimizer
}  // namespace project
