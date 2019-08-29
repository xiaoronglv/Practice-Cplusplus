#ifndef PROJECT_OPTIMIZER_PLAN_HPP_
#define PROJECT_OPTIMIZER_PLAN_HPP_

#include <memory>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/OptimizerTree.hpp"
#include "utility/TreeStringSerializable.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

enum class PlanType {
  kAggregate = 0,
  kChainedJoinAggregate,
  kEquiJoin,
  kEquiJoinAggregate,
  kFilter,
  kMultiwayCartesianJoin,
  kMultiwayEquiJoinAggregate,
  kProject,
  kSelection,
  kTableReference,
  kTableView,
  kTopLevelPlan
};

class Plan;
typedef std::shared_ptr<const Plan> PlanPtr;

class Plan : public OptimizerTree<Plan> {
 public:
  ~Plan() override {}

  virtual PlanType getPlanType() const = 0;

  virtual std::vector<AttributeReferencePtr> getOutputAttributes() const = 0;

  virtual std::vector<AttributeReferencePtr> getReferencedAttributes() const = 0;

  virtual bool maybeCopyWithPrunedAttributes(
      const UnorderedAttributeSet &referenced_attributes,
      PlanPtr *output) const {
    return false;
  }

 protected:
  Plan() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Plan);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_PLAN_HPP_
