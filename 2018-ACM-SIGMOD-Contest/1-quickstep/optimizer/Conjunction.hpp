#ifndef PROJECT_OPTIMIZER_CONJUNCTION_HPP_
#define PROJECT_OPTIMIZER_CONJUNCTION_HPP_

#include <string>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Predicate.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {
namespace optimizer {

class Conjunction;
typedef std::shared_ptr<const Conjunction> ConjunctionPtr;

class Conjunction : public Predicate {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kConjunction;
  }

  std::string getName() const override {
    return "And";
  }

  bool isConstant() const override;

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  Range reduceRange(const ExprId expr_id, const Range &input) const override;

  ::project::Predicate* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const override;

  std::string toShortString() const override;

  const std::vector<PredicatePtr>& operands() const {
    return operands_;
  }

  static ConjunctionPtr Create(const std::vector<PredicatePtr> &operands) {
    return ConjunctionPtr(new Conjunction(operands));
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
  explicit Conjunction(const std::vector<PredicatePtr> &operands)
      : operands_(operands) {
    for (const auto &operand : operands_) {
      addChild(operand);
    }
  }

  const std::vector<PredicatePtr> operands_;

  DISALLOW_COPY_AND_ASSIGN(Conjunction);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_CONJUNCTION_HPP_

