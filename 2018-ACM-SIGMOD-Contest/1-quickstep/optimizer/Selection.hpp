#ifndef PROJECT_OPTIMIZER_SELECTION_HPP_
#define PROJECT_OPTIMIZER_SELECTION_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class Selection;
typedef std::shared_ptr<const Selection> SelectionPtr;

class Selection : public Plan {
 public:
  enum SelectionType {
    kBasic,
    kIndexScan
  };

  PlanType getPlanType() const override {
    return PlanType::kSelection;
  }

  std::string getName() const override;

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  bool maybeCopyWithPrunedAttributes(
      const UnorderedAttributeSet &referenced_attributes,
      PlanPtr *output) const override;

  SelectionType selection_type() const {
    return selection_type_;
  }

  const PlanPtr& input() const {
    return input_;
  }

  const std::vector<ScalarPtr>& project_expressions() const {
    return project_expressions_;
  }

  const PredicatePtr& filter_predicate() const {
    return filter_predicate_;
  }

  static SelectionPtr Create(
      const PlanPtr &input,
      const std::vector<ScalarPtr> &project_expressions,
      const PredicatePtr &filter_predicate,
      const SelectionType selection_type = kBasic) {
    return SelectionPtr(new Selection(input,
                                      project_expressions,
                                      filter_predicate,
                                      selection_type));
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
  Selection(const PlanPtr &input,
            const std::vector<ScalarPtr> &project_expressions,
            const PredicatePtr filter_predicate,
            const SelectionType selection_type)
      : input_(input),
        project_expressions_(project_expressions),
        filter_predicate_(filter_predicate),
        selection_type_(selection_type) {
    addChild(input_);
  }

  const PlanPtr input_;
  const std::vector<ScalarPtr> project_expressions_;
  const PredicatePtr filter_predicate_;
  const SelectionType selection_type_;

  DISALLOW_COPY_AND_ASSIGN(Selection);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_PROJECT_HPP_
