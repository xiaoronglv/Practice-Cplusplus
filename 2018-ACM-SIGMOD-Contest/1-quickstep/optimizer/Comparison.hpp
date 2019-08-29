#ifndef PROJECT_OPTIMIZER_COMPARISON_HPP_
#define PROJECT_OPTIMIZER_COMPARISON_HPP_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "operators/expressions/ComparisonType.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

class Comparison;
typedef std::shared_ptr<const Comparison> ComparisonPtr;

class Comparison : public Predicate {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kComparison;
  }

  std::string getName() const override;

  bool isConstant() const override;

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  ::project::Predicate* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const override;

  std::string toShortString() const override;

  Range reduceRange(const ExprId expr_id, const Range &input) const override;

  ComparisonType comparison_type() const {
    return comparison_type_;
  }

  const ScalarPtr& left() const {
    return left_;
  }

  const ScalarPtr& right() const {
    return right_;
  }

  bool isAttributeToLiteralComparison() const {
    return left_->getExpressionType() == ExpressionType::kAttributeReference &&
           right_->getExpressionType() == ExpressionType::kScalarLiteral;
  }

  static ComparisonPtr Create(const ComparisonType &comparison_type,
                              const ScalarPtr &left,
                              const ScalarPtr &right) {
    return ComparisonPtr(new Comparison(comparison_type, left, right));
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
  Comparison(const ComparisonType comparison_type,
             const ScalarPtr &left,
             const ScalarPtr &right)
      : comparison_type_(comparison_type), left_(left), right_(right) {
    addChild(left_);
    addChild(right_);
  }

  const ComparisonType comparison_type_;
  const ScalarPtr left_;
  const ScalarPtr right_;

  DISALLOW_COPY_AND_ASSIGN(Comparison);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_COMPARISON_HPP_
