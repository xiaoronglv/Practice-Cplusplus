#ifndef PROJECT_OPTIMIZER_AGGREGATE_FUNCTION_HPP_
#define PROJECT_OPTIMIZER_AGGREGATE_FUNCTION_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/Scalar.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

enum class AggregateFunctionType {
  kSum = 0
};

class AggregateFunction;
typedef std::shared_ptr<const AggregateFunction> AggregateFunctionPtr;

class AggregateFunction : public Expression {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kAggregateFunction;
  }

  std::string getName() const override;

  const Type& getValueType() const override;

  bool isConstant() const override;

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  std::string toShortString() const override;

  AggregateFunctionType aggregate_type() const {
    return aggregate_type_;
  }

  const ScalarPtr& argument() const {
    return argument_;
  }

  static const AggregateFunctionPtr Create(
      const AggregateFunctionType aggregate_type,
      const ScalarPtr &argument) {
    return AggregateFunctionPtr(new AggregateFunction(aggregate_type, argument));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override;

 private:
  AggregateFunction(const AggregateFunctionType aggregate_type,
                    const ScalarPtr &argument)
      : aggregate_type_(aggregate_type), argument_(argument) {
    addChild(argument_);
  }

  const AggregateFunctionType aggregate_type_;
  const ScalarPtr argument_;

  DISALLOW_COPY_AND_ASSIGN(AggregateFunction);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_AGGREGATE_FUNCTION_HPP_
