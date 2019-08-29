#ifndef PROJECT_OPTIMIZER_MULTIWAY_CARTESIAN_JOIN_HPP_
#define PROJECT_OPTIMIZER_MULTIWAY_CARTESIAN_JOIN_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

class MultiwayCartesianJoin;
typedef std::shared_ptr<const MultiwayCartesianJoin> MultiwayCartesianJoinPtr;

class MultiwayCartesianJoin : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kMultiwayCartesianJoin;
  }

  std::string getName() const override {
    return "MultiwayCartesianJoin";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  const std::vector<PlanPtr>& operands() const {
    return operands_;
  }

  static MultiwayCartesianJoinPtr Create(const std::vector<PlanPtr> &operands) {
    DCHECK_GT(operands.size(), 1u);
    return MultiwayCartesianJoinPtr(new MultiwayCartesianJoin(operands));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override;

 private:
  explicit MultiwayCartesianJoin(const std::vector<PlanPtr> &operands)
      : operands_(operands) {
    for (const PlanPtr &operand : operands_) {
      addChild(operand);
    }
  }

  const std::vector<PlanPtr> operands_;

  DISALLOW_COPY_AND_ASSIGN(MultiwayCartesianJoin);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_MULTIWAY_CARTESIAN_JOIN_HPP_
