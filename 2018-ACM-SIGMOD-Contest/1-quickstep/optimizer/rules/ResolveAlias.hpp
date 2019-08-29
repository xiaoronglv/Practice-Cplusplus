#ifndef PROJECT_OPTIMIZER_RULES_RESOLVE_ALIAS_HPP_
#define PROJECT_OPTIMIZER_RULES_RESOLVE_ALIAS_HPP_

#include <string>
#include <unordered_map>

#include "optimizer/ExprId.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ResolveAlias : public BottomUpRule<Plan> {
 public:
  ResolveAlias() {}

  std::string getName() const override {
    return "ResolveAlias";
  }

 protected:
  PlanPtr applyToNode(const PlanPtr &node) override;

 private:
  std::unordered_map<ExprId, ScalarPtr> substitution_map_;

  DISALLOW_COPY_AND_ASSIGN(ResolveAlias);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_RESOLVE_ALIAS_HPP_
