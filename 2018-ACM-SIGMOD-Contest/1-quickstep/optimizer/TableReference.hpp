#ifndef PROJECT_OPTIMIZER_TABLE_REFERENCE_HPP_
#define PROJECT_OPTIMIZER_TABLE_REFERENCE_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/OptimizerContext.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class Relation;

namespace optimizer {

class TableReference;
typedef std::shared_ptr<const TableReference> TableReferencePtr;

class TableReference : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kTableReference;
  }

  std::string getName() const override {
    return "TableReference";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  const Relation& relation() const {
    return relation_;
  }

  const std::vector<AttributeReferencePtr>& attribute_list() const {
    return attribute_list_;
  }

  static TableReferencePtr Create(const Relation &relation,
                                  OptimizerContext *optimizer_context) {
    return TableReferencePtr(new TableReference(relation, optimizer_context));
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
  TableReference(const Relation &relation,
                 OptimizerContext *optimizer_context)
      : relation_(relation),
        attribute_list_(CreateAttributeList(relation, optimizer_context)) {}

  TableReference(const Relation &relation,
                 const std::vector<AttributeReferencePtr> &attribute_list)
      : relation_(relation),
        attribute_list_(attribute_list) {}

  static std::vector<AttributeReferencePtr> CreateAttributeList(
      const Relation &relation, OptimizerContext *optimizer_context);

  const Relation &relation_;
  const std::vector<AttributeReferencePtr> attribute_list_;

  DISALLOW_COPY_AND_ASSIGN(TableReference);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_TABLE_REFERENCE_HPP_
