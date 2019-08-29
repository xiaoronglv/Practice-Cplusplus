#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_INDEX_JOIN_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_INDEX_JOIN_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializePrimaryKeyIndexJoin : public BottomUpRule<Plan> {
 public:
  explicit SpecializePrimaryKeyIndexJoin(
      const std::size_t cardinality_threshold)
      : cardinality_threshold_(cardinality_threshold) {}

  std::string getName() const override {
    return "SpecializePrimaryKeyIndexJoin";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  const std::size_t cardinality_threshold_;
  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(SpecializePrimaryKeyIndexJoin);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_INDEX_JOIN_HPP_
