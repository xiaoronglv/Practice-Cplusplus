#include "optimizer/rules/UpdateExpression.hpp"

#include <unordered_map>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/PatternMatcher.hpp"

namespace project {
namespace optimizer {

UpdateExpression::UpdateExpression(
    const std::unordered_map<ExprId, ExpressionPtr> &substitution_map)
    : substitution_map_(substitution_map) {}

ExpressionPtr UpdateExpression::applyToNode(const ExpressionPtr &input) {
  AttributeReferencePtr attribute;

  if (SomeAttributeReference::MatchesWithConditionalCast(input, &attribute)) {
    const auto it = substitution_map_.find(attribute->id());
    if (it != substitution_map_.end()) {
      return it->second;
    }
  }
  return input;
}

}  // namespace optimizer
}  // namespace project
