#ifndef PROJECT_OPTIMIZER_RULES_CONSTANT_PROPAGATION_HPP_
#define PROJECT_OPTIMIZER_RULES_CONSTANT_PROPAGATION_HPP_

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "optimizer/ExprId.hpp"
#include "optimizer/OptimizerContext.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ConstantPropagation : public Rule<Plan> {
 public:
  explicit ConstantPropagation(OptimizerContext *optimizer_context)
      : optimizer_context_(optimizer_context) {}

  std::string getName() const override {
    return "ConstantPropagation";
  }

  PlanPtr apply(const PlanPtr &input) override;

 private:
  void collectConstants(const PlanPtr &node);

  void collectConstant(const PlanPtr &source,
                       const ExprId expr_id,
                       const std::uint64_t value);

  PlanPtr attachFilters(const PlanPtr &node,
                        std::unordered_set<ExprId> *filtered);

  OptimizerContext *optimizer_context_;

  std::unordered_map<ExprId, std::uint64_t> constants_;
  std::unordered_map<PlanPtr, std::unordered_set<ExprId>> source_;

  const SimpleCostModel cost_model_;

  static constexpr std::uint64_t kInvalidConstant =
      std::numeric_limits<std::uint64_t>::max();

  DISALLOW_COPY_AND_ASSIGN(ConstantPropagation);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_CONSTANT_PROPAGATION_HPP_
