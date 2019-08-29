#ifndef PROJECT_OPTIMIZER_ALIAS_HPP_
#define PROJECT_OPTIMIZER_ALIAS_HPP_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/Scalar.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class Alias;
typedef std::shared_ptr<const Alias> AliasPtr;

class Alias : public Scalar {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kAlias;
  }

  std::string getName() const override {
    return "Alias";
  }

  const Type& getValueType() const override {
    return expression_->getValueType();
  }

  bool isConstant() const override {
    return false;
  }

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  AttributeReferencePtr getAttribute() const override;

  std::string toShortString() const override;

  ::project::Scalar* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const override;

  const ExpressionPtr& expression() const {
    return expression_;
  }

  ExprId id() const {
    return id_;
  }

  const std::string& name() const {
    return name_;
  }

  static const AliasPtr Create(const ExpressionPtr &expression,
                               const ExprId id,
                               const std::string &name) {
    return AliasPtr(new Alias(expression, id, name));
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
  Alias(const ExpressionPtr &expression,
        const ExprId id,
        const std::string &name)
      : expression_(expression), id_(id), name_(name) {
    addChild(expression_);
  }

  const ExpressionPtr expression_;
  const ExprId id_;
  const std::string name_;

  DISALLOW_COPY_AND_ASSIGN(Alias);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_ALIAS_HPP_
