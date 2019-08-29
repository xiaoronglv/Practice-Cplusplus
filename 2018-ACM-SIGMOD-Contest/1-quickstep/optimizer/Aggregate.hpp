#ifndef PROJECT_OPTIMIZER_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_AGGREGATE_HPP_

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

class Aggregate;
typedef std::shared_ptr<const Aggregate> AggregatePtr;

class Aggregate : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kAggregate;
  }

  std::string getName() const override {
    return "Aggregate";
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

  const std::vector<ScalarPtr>& aggregate_expressions() const {
    return aggregate_expressions_;
  }

  const PredicatePtr filter_predicate() const {
    return filter_predicate_;
  }

  static AggregatePtr Create(
      const PlanPtr &input,
      const std::vector<ScalarPtr> &aggregate_expressions,
      const PredicatePtr &filter_predicate) {
    return AggregatePtr(new Aggregate(input, aggregate_expressions, filter_predicate));
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
  Aggregate(const PlanPtr &input,
            const std::vector<ScalarPtr> &aggregate_expressions,
            const PredicatePtr &filter_predicate)
      : input_(input),
        aggregate_expressions_(aggregate_expressions),
        filter_predicate_(filter_predicate) {
    addChild(input_);
  }

  const PlanPtr input_;
  const std::vector<ScalarPtr> aggregate_expressions_;
  const PredicatePtr filter_predicate_;

  DISALLOW_COPY_AND_ASSIGN(Aggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_AGGREGATE_HPP_
