#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_SCAN_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_SCAN_JOIN_AGGREGATE_HPP_

#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializePrimaryKeyScanJoinAggregate : public BottomUpRule<Plan> {
 public:
  SpecializePrimaryKeyScanJoinAggregate() {}

  std::string getName() const override {
    return "SpecializePrimaryKeyScanJoinAggregate";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;

  void rewriteAggregateExpressions(
      const ExprId probe_attribute, const AttributeReferencePtr &build_attribute,
      std::vector<ScalarPtr> *aggregate_expressions);

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(SpecializePrimaryKeyScanJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_SCAN_JOIN_AGGREGATE_HPP_
