#ifndef PROJECT_OPTIMIZER_RULES_TOP_DOWN_RULE_HPP_
#define PROJECT_OPTIMIZER_RULES_TOP_DOWN_RULE_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

template <typename TreeType>
class TopDownRule : public Rule<TreeType> {
 private:
  typedef std::shared_ptr<const TreeType> TreeNodePtr;

 public:
  TreeNodePtr apply(const TreeNodePtr &input) override {
    DCHECK(input != nullptr);

    init(input);
    return applyInternal(input);
  }

 protected:
  TopDownRule() {}

  virtual TreeNodePtr applyToNode(const PlanPtr &node) = 0;

  virtual void init(const TreeNodePtr &input) {}

 private:
  TreeNodePtr applyInternal(const TreeNodePtr &node) {
    DCHECK(node != nullptr);
    const TreeNodePtr new_node = applyToNode(node);

    std::vector<TreeNodePtr> new_children;
    new_children.reserve(new_node->getNumChildren());
    for (const TreeNodePtr &child : new_node->children()) {
      new_children.emplace_back(applyInternal(child));
    }

    if (new_children == new_node->children()) {
      return new_node;
    } else {
      return new_node->copyWithNewChildren(new_children);
    }
  }

  DISALLOW_COPY_AND_ASSIGN(TopDownRule);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_TOP_DOWN_RULE_HPP_
