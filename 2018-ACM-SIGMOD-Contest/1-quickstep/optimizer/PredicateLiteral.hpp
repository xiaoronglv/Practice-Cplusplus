#ifndef PROJECT_OPTIMIZER_PREDICATE_LITERAL_HPP_
#define PROJECT_OPTIMIZER_PREDICATE_LITERAL_HPP_

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Predicate.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {
namespace optimizer {

class PredicateLiteral;
typedef std::shared_ptr<const PredicateLiteral> PredicateLiteralPtr;

class PredicateLiteral : public Predicate {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kPredicateLiteral;
  }

  std::string getName() const override {
    return "PredicateLiteral";
  }

  bool isConstant() const override {
    return true;
  }

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  Range reduceRange(const ExprId expr_id, const Range &input) const override;

  ::project::Predicate* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const override;

  std::string toShortString() const override {
    return value_ ? "true" : "false";
  }

  const bool value() const {
    return value_;
  }

  static PredicatePtr Create(const bool value) {
    return PredicateLiteralPtr(new PredicateLiteral(value));
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
  explicit PredicateLiteral(bool value)
      : value_(value) {}

  bool value_;

  DISALLOW_COPY_AND_ASSIGN(PredicateLiteral);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_PREDICATE_LITERAL_HPP_
