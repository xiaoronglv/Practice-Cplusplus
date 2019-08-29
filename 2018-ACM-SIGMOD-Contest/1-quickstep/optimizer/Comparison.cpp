#include "optimizer/Comparison.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "operators/expressions/Comparison.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/ScalarLiteral.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::string Comparison::getName() const {
  switch (comparison_type_) {
    case ComparisonType::kEqual:
      return "Equal";
    case ComparisonType::kLess:
      return "Less";
    case ComparisonType::kGreater:
      return "Greater";
    default:
      break;
  }
  return "UnknownComparison";
}

bool Comparison::isConstant() const {
  return left_->isConstant() && right_->isConstant();
}

ExpressionPtr Comparison::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  ScalarPtr left_operand;
  ScalarPtr right_operand;
  CHECK(SomeScalar::MatchesWithConditionalCast(new_children[0], &left_operand));
  CHECK(SomeScalar::MatchesWithConditionalCast(new_children[1], &right_operand));
  return Create(comparison_type_, left_operand, right_operand);
}

std::vector<AttributeReferencePtr> Comparison::getReferencedAttributes() const {
  return Concatenate(left_->getReferencedAttributes(),
                     right_->getReferencedAttributes());
}

::project::Predicate* Comparison::concretize(
    const std::unordered_map<ExprId, const Attribute*> &substitution_map) const {
  return new ::project::Comparison(comparison_type_,
                                   left_->concretize(substitution_map),
                                   right_->concretize(substitution_map));
}

std::string Comparison::toShortString() const {
  std::string op;
  switch (comparison_type_) {
    case ComparisonType::kEqual:
      op = "=";
      break;
    case ComparisonType::kLess:
      op = "<";
      break;
    case ComparisonType::kGreater:
      op = ">";
      break;
  }
  return left_->toShortString() + " " + op + " " + right_->toShortString();
}

Range Comparison::reduceRange(const ExprId expr_id, const Range &input) const {
  if (!isAttributeToLiteralComparison() || input.size() == 0) {
    return input;
  }

  AttributeReferencePtr attribute;
  CHECK(SomeAttributeReference::MatchesWithConditionalCast(left_, &attribute));
  if (attribute->id() != expr_id) {
    return input;
  }

  const std::uint64_t literal =
      static_cast<const ScalarLiteral&>(*right_).value();

  const std::uint64_t begin = input.begin();
  const std::uint64_t end = input.end();

  switch (comparison_type_) {
    case ComparisonType::kEqual:
      if (literal >= begin && literal < end) {
        return Range(literal, literal+1);
      }
      break;
    case ComparisonType::kLess:
      if (literal > begin) {
        return Range(begin, std::min(literal, end));
      }
      break;
    case ComparisonType::kGreater:
      if (literal+1 < end) {
        return Range(std::max(begin, literal+1), end);
      }
      break;
    default:
      break;
  }
  return Range();
}

void Comparison::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("");
  non_container_child_fields->emplace_back(left_);

  non_container_child_field_names->emplace_back("");
  non_container_child_fields->emplace_back(right_);
}

}  // namespace optimizer
}  // namespace project
