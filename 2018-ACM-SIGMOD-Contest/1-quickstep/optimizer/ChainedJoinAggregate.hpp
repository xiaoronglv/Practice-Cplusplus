#ifndef PROJECT_OPTIMIZER_CHAINED_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_CHAINED_JOIN_AGGREGATE_HPP_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SimpleCostModel;

class ChainedJoinAggregate;
typedef std::shared_ptr<const ChainedJoinAggregate> ChainedJoinAggregatePtr;

class ChainedJoinAggregate : public Plan {
 public:
  // pair of (build, probe) attributes
  using AttributePair = std::pair<AttributeReferencePtr, AttributeReferencePtr>;

  PlanType getPlanType() const override {
    return PlanType::kChainedJoinAggregate;
  }

  std::string getName() const override {
    return "ChainedJoinAggregate";
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

  const std::vector<AttributePair>& join_attribute_pairs() const {
    return join_attribute_pairs_;
  }

  const std::vector<ScalarPtr>& aggregate_expressions() const {
    return aggregate_expressions_;
  }

  const std::vector<PredicatePtr>& filter_predicates() const {
    return filter_predicates_;
  }

  static ChainedJoinAggregatePtr Create(
      const std::vector<PlanPtr> &inputs,
      const std::vector<AttributePair> &join_attribute_pairs,
      const std::vector<ScalarPtr> &aggregate_expressions,
      const std::vector<PredicatePtr> &filter_predicates) {
    return ChainedJoinAggregatePtr(
        new ChainedJoinAggregate(inputs, join_attribute_pairs,
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
  ChainedJoinAggregate(const std::vector<PlanPtr> &inputs,
                       const std::vector<AttributePair> &join_attribute_pairs,
                       const std::vector<ScalarPtr> &aggregate_expressions,
                       const std::vector<PredicatePtr> &filter_predicates)
      : inputs_(inputs),
        join_attribute_pairs_(join_attribute_pairs),
        aggregate_expressions_(aggregate_expressions),
        filter_predicates_(filter_predicates) {
    DCHECK_EQ(inputs.size() - 1u, join_attribute_pairs.size());
    DCHECK_EQ(inputs.size(), filter_predicates.size());

    for (const auto &input : inputs_) {
      addChild(input);
    }
  }

  // { build, probes... }
  const std::vector<PlanPtr> inputs_;
  const std::vector<AttributePair> join_attribute_pairs_;
  const std::vector<ScalarPtr> aggregate_expressions_;
  // { build, probes... }
  const std::vector<PredicatePtr> filter_predicates_;

  DISALLOW_COPY_AND_ASSIGN(ChainedJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_CHAINED_JOIN_AGGREGATE_HPP_
