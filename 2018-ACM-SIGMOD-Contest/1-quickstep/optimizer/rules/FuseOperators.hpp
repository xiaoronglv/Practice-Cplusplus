#ifndef PROJECT_OPTIMIZER_RULES_FUSE_OPERATORS_HPP_
#define PROJECT_OPTIMIZER_RULES_FUSE_OPERATORS_HPP_

#include <string>

#include "optimizer/Aggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class FuseOperators : public BottomUpRule<Plan> {
 public:
  FuseOperators() {}

  std::string getName() const override {
    return "FuseOperators";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;
  PlanPtr applyToAggregate(const AggregatePtr &aggregate) const;
  PlanPtr applyToEquiJoin(const EquiJoinPtr &equi_join) const;

  PlanPtr fuseAggregateSelection(const AggregatePtr &aggregate,
                                 const SelectionPtr &selection) const;

  const SimpleCostModel cost_model_;

  static constexpr std::uint64_t kHashJoinCardinalityThreshold = 50000u;

  DISALLOW_COPY_AND_ASSIGN(FuseOperators);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_FUSE_OPERATORS_HPP_
