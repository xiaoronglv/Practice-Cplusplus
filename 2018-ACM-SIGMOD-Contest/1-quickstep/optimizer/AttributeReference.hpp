#ifndef PROJECT_OPTIMIZER_ATTRIBUTE_REFERENCE_HPP_
#define PROJECT_OPTIMIZER_ATTRIBUTE_REFERENCE_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "operators/expressions/ScalarAttribute.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Scalar.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class AttributeReference;
typedef std::shared_ptr<const AttributeReference> AttributeReferencePtr;

class AttributeReference : public Scalar {
 public:
  ExpressionType getExpressionType() const override {
    return ExpressionType::kAttributeReference;
  }

  std::string getName() const override {
    return "AttributeReference";
  }

  const Type& getValueType() const override {
    return type_;
  }

  bool isConstant() const override {
    return false;
  }

  ExpressionPtr copyWithNewChildren(
      const std::vector<ExpressionPtr> &new_children) const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  AttributeReferencePtr getAttribute() const override;

  std::string toShortString() const override {
    return name_;
  }

  ::project::Scalar* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const override;

  ExprId id() const {
    return id_;
  }

  const std::string& name() const {
    return name_;
  }

  static const AttributeReferencePtr Create(const ExprId id,
                                            const Type &type,
                                            const std::string &name) {
    return AttributeReferencePtr(new AttributeReference(id, type, name));
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
  AttributeReference(const ExprId id,
                     const Type &type,
                     const std::string &name)
      : id_(id), type_(type), name_(name) {}

  const ExprId id_;
  const Type &type_;
  const std::string name_;

  DISALLOW_COPY_AND_ASSIGN(AttributeReference);
};

struct AttributeReferenceEqual {
  inline bool operator()(const AttributeReferencePtr &lhs,
                         const AttributeReferencePtr &rhs) const {
    return lhs->id() == rhs->id();
  }
};

struct AttributeReferenceHash {
  inline std::size_t operator()(const AttributeReferencePtr &attr) const {
    return std::hash<ExprId>()(attr->id());
  }
};

using UnorderedAttributeSet =
    std::unordered_set<AttributeReferencePtr, AttributeReferenceHash, AttributeReferenceEqual>;

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_ATTRIBUTE_REFERENCE_HPP_

