#ifndef PROJECT_OPTIMIZER_MULTIWAY_EQUI_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_MULTIWAY_EQUI_JOIN_AGGREGATE_HPP_

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

class MultiwayEquiJoinAggregate;
typedef std::shared_ptr<const MultiwayEquiJoinAggregate> MultiwayEquiJoinAggregatePtr;

class MultiwayEquiJoinAggregate : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kMultiwayEquiJoinAggregate;
  }

  std::string getName() const override {
    return "MultiwayEquiJoinAggregate";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  bool maybeCopyWithPrunedAttributes(
      const UnorderedAttributeSet &referenced_attributes,
      PlanPtr *output) const override;

  const std::vector<PlanPtr>& inputs() const {
    return inputs_;
  }

  const std::vector<AttributeReferencePtr>& join_attributes() const {
    return join_attributes_;
  }

  const std::vector<ScalarPtr>& aggregate_expressions() const {
    return aggregate_expressions_;
  }

  const std::vector<PredicatePtr>& filter_predicates() const {
    return filter_predicates_;
  }

  static MultiwayEquiJoinAggregatePtr Create(
      const std::vector<PlanPtr> &inputs,
      const std::vector<AttributeReferencePtr> &join_attributes,
      const std::vector<ScalarPtr> &aggregate_expressions,
      const std::vector<PredicatePtr> &filter_predicates) {
    return MultiwayEquiJoinAggregatePtr(
        new MultiwayEquiJoinAggregate(inputs,
                                      join_attributes,
                                      aggregate_expressions,
                                      filter_predicates));
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
  MultiwayEquiJoinAggregate(
      const std::vector<PlanPtr> &inputs,
      const std::vector<AttributeReferencePtr> &join_attributes,
      const std::vector<ScalarPtr> &aggregate_expressions,
      const std::vector<PredicatePtr> &filter_predicates)
      : inputs_(inputs),
        join_attributes_(join_attributes),
        aggregate_expressions_(aggregate_expressions),
        filter_predicates_(filter_predicates) {
    for (const auto &input : inputs_) {
      addChild(input);
    }
  }

  const std::vector<PlanPtr> inputs_;
  const std::vector<AttributeReferencePtr> join_attributes_;
  const std::vector<ScalarPtr> aggregate_expressions_;
  const std::vector<PredicatePtr> filter_predicates_;

  DISALLOW_COPY_AND_ASSIGN(MultiwayEquiJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_MULTIWAY_EQUI_JOIN_AGGREGATE_HPP_
