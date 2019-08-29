#ifndef PROJECT_OPTIMIZER_SCALAR_LITERAL_HPP_
#define PROJECT_OPTIMIZER_SCALAR_LITERAL_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Scalar.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

class ScalarLiteral;
typedef std::shared_ptr<const ScalarLiteral> ScalarLiteralPtr;

class ScalarLiteral : public Scalar {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kScalarLiteral;
  }

  std::string getName() const override {
    return "ScalarLiteral";
  }

  const Type& getValueType() const override {
    return value_type_;
  }

  bool isConstant() const override {
    return true;
  }

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  AttributeReferencePtr getAttribute() const override;

  std::string toShortString() const override;

  ::project::Scalar* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const override;

  std::uint64_t value() const {
    return value_;
  }

  static const ScalarLiteralPtr Create(const std::uint64_t value,
                                       const Type &value_type,
                                       const ExprId expr_id) {
    return ScalarLiteralPtr(new ScalarLiteral(value, value_type, expr_id));
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
  ScalarLiteral(const std::uint64_t value,
                const Type &value_type,
                const ExprId expr_id)
      : value_(value), value_type_(value_type), expr_id_(expr_id) {}

  const std::uint64_t value_;
  const Type &value_type_;
  const ExprId expr_id_;

  DISALLOW_COPY_AND_ASSIGN(ScalarLiteral);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_SCALAR_LITERAL_HPP_
