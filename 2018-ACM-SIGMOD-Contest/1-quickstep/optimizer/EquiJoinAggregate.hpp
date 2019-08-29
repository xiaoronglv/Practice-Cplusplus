#ifndef PROJECT_OPTIMIZER_EQUI_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_EQUI_JOIN_AGGREGATE_HPP_

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

class EquiJoinAggregate;
typedef std::shared_ptr<const EquiJoinAggregate> EquiJoinAggregatePtr;

class EquiJoinAggregate : public Plan {
 public:
  enum JoinType {
    kBuildSideForeignKeyIndex = 0,
    kForeignKeyIndexPrimaryKeyIndex,
    kForeignKeyPrimaryKeyIndex,
    kForeignKeyPrimaryKeyScan,
  };

  PlanType getPlanType() const override {
    return PlanType::kEquiJoinAggregate;
  }

  std::string getName() const override;

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  bool maybeCopyWithPrunedAttributes(
      const UnorderedAttributeSet &referenced_attributes,
      PlanPtr *output) const override;

  const PlanPtr& probe() const {
    return probe_;
  }

  const PlanPtr& build() const {
    return build_;
  }

  const std::vector<AttributeReferencePtr>& probe_attributes() const {
    return probe_attributes_;
  }

  const std::vector<AttributeReferencePtr>& build_attributes() const {
    return build_attributes_;
  }

  const std::vector<ScalarPtr>& aggregate_expressions() const {
    return aggregate_expressions_;
  }

  const PredicatePtr& probe_filter_predicate() const {
    return probe_filter_predicate_;
  }

  const PredicatePtr& build_filter_predicate() const {
    return build_filter_predicate_;
  }

  JoinType join_type() const {
    return join_type_;
  }

  static EquiJoinAggregatePtr Create(
      const PlanPtr &probe,
      const PlanPtr &build,
      const std::vector<AttributeReferencePtr> &probe_attributes,
      const std::vector<AttributeReferencePtr> &build_attributes,
      const std::vector<ScalarPtr> &aggregate_expressions,
      const PredicatePtr &probe_filter_predicate,
      const PredicatePtr &build_filter_predicate,
      const JoinType join_type) {
    return EquiJoinAggregatePtr(
        new EquiJoinAggregate(probe, build,
                              probe_attributes,
                              build_attributes,
                              aggregate_expressions,
                              probe_filter_predicate,
                              build_filter_predicate,
                              join_type));
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
  EquiJoinAggregate(const PlanPtr &probe,
                    const PlanPtr &build,
                    const std::vector<AttributeReferencePtr> &probe_attributes,
                    const std::vector<AttributeReferencePtr> &build_attributes,
                    const std::vector<ScalarPtr> &aggregate_expressions,
                    const PredicatePtr &probe_filter_predicate,
                    const PredicatePtr &build_filter_predicate,
                    const JoinType join_type)
      : probe_(probe), build_(build),
        probe_attributes_(probe_attributes),
        build_attributes_(build_attributes),
        aggregate_expressions_(aggregate_expressions),
        probe_filter_predicate_(probe_filter_predicate),
        build_filter_predicate_(build_filter_predicate),
        join_type_(join_type) {
    addChild(probe_);
    addChild(build_);
  }

  const PlanPtr probe_;
  const PlanPtr build_;
  const std::vector<AttributeReferencePtr> probe_attributes_;
  const std::vector<AttributeReferencePtr> build_attributes_;
  const std::vector<ScalarPtr> aggregate_expressions_;
  const PredicatePtr probe_filter_predicate_;
  const PredicatePtr build_filter_predicate_;
  const JoinType join_type_;

  DISALLOW_COPY_AND_ASSIGN(EquiJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_EQUI_JOIN_AGGREGATE_HPP_
