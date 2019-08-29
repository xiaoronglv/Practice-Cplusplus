#ifndef PROJECT_OPTIMIZER_FILTER_HPP_
#define PROJECT_OPTIMIZER_FILTER_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class Filter;
typedef std::shared_ptr<const Filter> FilterPtr;

class Filter : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kFilter;
  }

  std::string getName() const override {
    return "Filter";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  const PlanPtr& input() const {
    return input_;
  }

  const PredicatePtr& filter_predicate() const {
    return filter_predicate_;
  }

  static FilterPtr Create(const PlanPtr &input,
                          const PredicatePtr &filter_predicate) {
    return FilterPtr(new Filter(input, filter_predicate));
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
  Filter(const PlanPtr &input, const PredicatePtr &filter_predicate)
      : input_(input),
        filter_predicate_(filter_predicate) {
    addChild(input_);
  }

  const PlanPtr input_;
  const PredicatePtr filter_predicate_;

  DISALLOW_COPY_AND_ASSIGN(Filter);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_FILTER_HPP_
