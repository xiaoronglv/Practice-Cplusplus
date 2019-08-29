#include "optimizer/AggregateFunction.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Scalar.hpp"
#include "types/Type.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

std::string AggregateFunction::getName() const {
  switch (aggregate_type_) {
    case AggregateFunctionType::kSum:
      return "Sum";
    default:
      break;
  }
  return "UnknownAggregateFunction";
}

const Type& AggregateFunction::getValueType() const {
  return UInt64Type::Instance();
}

bool AggregateFunction::isConstant() const {
  return false;
}

ExpressionPtr AggregateFunction::copyWithNewChildren(
    const std::vector<ExpressionPtr> &new_children) const {
  DCHECK_EQ(new_children.size(), children().size());
  ScalarPtr scalar;
  CHECK(SomeScalar::MatchesWithConditionalCast(new_children[0], &scalar));
  return AggregateFunction::Create(aggregate_type_, scalar);
}

std::vector<AttributeReferencePtr> AggregateFunction::getReferencedAttributes() const {
  return argument_->getReferencedAttributes();
}

std::string AggregateFunction::toShortString() const {
  return getName() + "([" + std::to_string(argument_->getAttribute()->id()) + "] " +
         argument_->toShortString() + ")";
}

void AggregateFunction::getFieldStringItems(
    std::vector<std::string> *inline_field_names,
    std::vector<std::string> *inline_field_values,
    std::vector<std::string> *non_container_child_field_names,
    std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
    std::vector<std::string> *container_child_field_names,
    std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const {
  non_container_child_field_names->emplace_back("argument");
  non_container_child_fields->emplace_back(argument_);
}

}  // namespace optimizer
}  // namespace project
