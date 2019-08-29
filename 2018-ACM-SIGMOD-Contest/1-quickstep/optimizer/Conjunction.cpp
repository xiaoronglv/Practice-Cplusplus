#include "optimizer/Conjunction.hpp"

#include <string>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "operators/expressions/Conjunction.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"
#include "utility/Range.hpp"
#include "utility/StringUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

bool Conjunction::isConstant() const {
  for (const auto &operand : operands_) {
    if (!operand->isConstant()) {
      return false;
    }
  }
  return true;
}

ExpressionPtr Conjunction::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  std::vector<PredicatePtr> operands;
  for (const ExpressionPtr &expression : new_children) {
    PredicatePtr predicate;
    CHECK(SomePredicate::MatchesWithConditionalCast(expression, &predicate));
    operands.push_back(predicate);
  }
  return Create(operands);
}

std::vector<AttributeReferencePtr> Conjunction::getReferencedAttributes() const {
  std::vector<AttributeReferencePtr> referenced_attributes;
  for (const auto &operand : operands_) {
    InsertAll(operand->getReferencedAttributes(), &referenced_attributes);
  }
  return referenced_attributes;
}

Range Conjunction::reduceRange(const ExprId expr_id, const Range &input) const {
  Range output = input;
  for (const auto &operand : operands_) {
    output = operand->reduceRange(expr_id, output);
  }
  return output;
}

::project::Predicate* Conjunction::concretize(
    const std::unordered_map<ExprId, const Attribute*> &substitution_map) const {
  std::vector<std::unique_ptr<::project::Predicate>> operands;
  for (const auto &operand : operands_) {
    operands.emplace_back(operand->concretize(substitution_map));
  }
  return new ::project::Conjunction(std::move(operands));
}

std::string Conjunction::toShortString() const {
  std::vector<std::string> strs;
  for (const auto &operand : operands_) {
    strs.emplace_back(operand->toShortString());
  }
  return ConcatToString(strs, " & ");
}

void Conjunction::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  container_child_field_names->emplace_back("");
  container_child_fields->emplace_back(
      CastSharedPtrVector<OptimizerTreeBase>(operands_));
}

}  // namespace optimizer
}  // namespace project
