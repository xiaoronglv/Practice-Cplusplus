#ifndef PROJECT_OPTIMIZER_RULES_UPDATE_EXPRESSION_HPP_
#define PROJECT_OPTIMIZER_RULES_UPDATE_EXPRESSION_HPP_

#include <string>
#include <unordered_map>

#include "optimizer/Expression.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class UpdateExpression : public BottomUpRule<Expression> {
 public:
  explicit UpdateExpression(const std::unordered_map<ExprId, ExpressionPtr> &substitution_map);

  std::string getName() const override {
    return "UpdateExpression";
  }

 protected:
  ExpressionPtr applyToNode(const ExpressionPtr &node) override;

 private:
  const std::unordered_map<ExprId, ExpressionPtr> &substitution_map_;

  DISALLOW_COPY_AND_ASSIGN(UpdateExpression);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_UPDATE_EXPRESSION_HPP_
