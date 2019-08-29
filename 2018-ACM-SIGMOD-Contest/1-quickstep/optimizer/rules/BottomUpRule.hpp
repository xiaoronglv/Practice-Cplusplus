#ifndef PROJECT_OPTIMIZER_RULES_BOTTOM_UP_RULE_HPP_
#define PROJECT_OPTIMIZER_RULES_BOTTOM_UP_RULE_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

template <typename TreeType>
class BottomUpRule : public Rule<TreeType> {
 private:
  typedef std::shared_ptr<const TreeType> TreeNodePtr;

 public:
  TreeNodePtr apply(const TreeNodePtr &input) override {
    DCHECK(input != nullptr);

    init(input);
    return applyInternal(input);
  }

 protected:
  BottomUpRule() {}

  virtual TreeNodePtr applyToNode(const TreeNodePtr &node) = 0;

  virtual void init(const TreeNodePtr &input) {}

 private:
  TreeNodePtr applyInternal(const TreeNodePtr &node) {
    DCHECK(node != nullptr);

    std::vector<TreeNodePtr> new_children;
    new_children.reserve(node->getNumChildren());
    for (const TreeNodePtr &child : node->children()) {
      new_children.emplace_back(applyInternal(child));
    }

    if (new_children == node->children()) {
      return applyToNode(node);
    } else {
      return applyToNode(node->copyWithNewChildren(new_children));
    }
  }

  DISALLOW_COPY_AND_ASSIGN(BottomUpRule);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_BOTTOM_UP_RULE_HPP_
