#ifndef PROJECT_OPTIMIZER_OPTIMIZER_TREE_HPP_
#define PROJECT_OPTIMIZER_OPTIMIZER_TREE_HPP_

#include <memory>
#include <vector>

#include "utility/TreeStringSerializable.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class OptimizerTreeBase
    : public TreeStringSerializable<std::shared_ptr<const OptimizerTreeBase>> {
 public:
  typedef std::shared_ptr<const OptimizerTreeBase> OptimizerTreeBaseNodePtr;

  ~OptimizerTreeBase() override {}

 protected:
  OptimizerTreeBase() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(OptimizerTreeBase);
};

template <typename NodeType>
class OptimizerTree : public OptimizerTreeBase {
 public:
  typedef std::shared_ptr<const NodeType> OptimizerTreeNodePtr;

  ~OptimizerTree() override {}

  const std::vector<OptimizerTreeNodePtr>& children() const {
    return children_;
  }

  std::size_t getNumChildren() const {
    return children_.size();
  }

  virtual OptimizerTreeNodePtr copyWithNewChildren(
      const std::vector<OptimizerTreeNodePtr> &new_children) const = 0;

 protected:
  OptimizerTree() {}

  void addChild(const OptimizerTreeNodePtr &child) {
    children_.emplace_back(child);
  }

 private:
  std::vector<OptimizerTreeNodePtr> children_;

  DISALLOW_COPY_AND_ASSIGN(OptimizerTree);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_OPTIMIZER_TREE_HPP_
