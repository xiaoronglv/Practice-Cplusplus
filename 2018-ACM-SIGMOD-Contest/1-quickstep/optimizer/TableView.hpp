#ifndef PROJECT_OPTIMIZER_TABLE_VIEW_HPP_
#define PROJECT_OPTIMIZER_TABLE_VIEW_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/Plan.hpp"
#include "storage/AttributeStatistics.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class TableView;
typedef std::shared_ptr<const TableView> TableViewPtr;

class TableView : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kTableView;
  }

  std::string getName() const override {
    return "TableView";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  bool maybeCopyWithPrunedAttributes(
      const UnorderedAttributeSet &referenced_attributes,
      PlanPtr *output) const override;

  const PlanPtr& input() const {
    return input_;
  }

  const ComparisonPtr& comparison() const {
    return comparison_;
  }

  SortOrder sort_order() const {
    return sort_order_;
  }

  static TableViewPtr Create(const PlanPtr &input,
                             const ComparisonPtr &comparison,
                             const SortOrder sort_order) {
    return TableViewPtr(new TableView(input, comparison, sort_order));
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
  TableView(const PlanPtr &input,
            const ComparisonPtr &comparison,
            const SortOrder sort_order)
      : input_(input),
        comparison_(comparison),
        sort_order_(sort_order) {
    addChild(input_);
  }

  const PlanPtr input_;
  const ComparisonPtr comparison_;
  const SortOrder sort_order_;

  DISALLOW_COPY_AND_ASSIGN(TableView);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_TABLE_VIEW_HPP_
