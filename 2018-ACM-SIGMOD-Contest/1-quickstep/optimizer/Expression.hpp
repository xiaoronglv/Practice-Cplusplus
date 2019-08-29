#ifndef PROJECT_OPTIMIZER_EXPRESSION_HPP_
#define PROJECT_OPTIMIZER_EXPRESSION_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/OptimizerTree.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

enum class ExpressionType {
  kAggregateFunction = 0,
  kAlias,
  kAttributeReference,
  kComparison,
  kConjunction,
  kPredicateLiteral,
  kScalarLiteral
};

class AttributeReference;
class Expression;
typedef std::shared_ptr<const Expression> ExpressionPtr;

class Expression : public OptimizerTree<Expression> {
 public:
  virtual ExpressionType getExpressionType() const = 0;

  virtual const Type& getValueType() const = 0;

  virtual std::vector<std::shared_ptr<const AttributeReference>> getReferencedAttributes() const = 0;

  virtual bool isConstant() const = 0;

  virtual std::string toShortString() const = 0;

 protected:
  Expression() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Expression);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_EXPRESSION_HPP_
