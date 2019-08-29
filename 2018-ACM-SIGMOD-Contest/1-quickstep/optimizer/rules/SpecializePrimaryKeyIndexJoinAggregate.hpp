#ifndef PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_INDEX_JOIN_AGGREGATE_HPP_
#define PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_INDEX_JOIN_AGGREGATE_HPP_

#include <string>

#include "optimizer/EquiJoin.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "optimizer/rules/BottomUpRule.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class SpecializePrimaryKeyIndexJoinAggregate : public BottomUpRule<Plan> {
 public:
  explicit SpecializePrimaryKeyIndexJoinAggregate(const Database &database)
      : database_(database) {}

  std::string getName() const override {
    return "SpecializePrimaryKeyIndexJoinAggregate";
  }

 private:
  PlanPtr applyToNode(const PlanPtr &node) override;
  bool canUseForeignKeyIndex(const EquiJoinPtr &equi_join,
                             const PlanPtr &probe,
                             const PredicatePtr &predicate) const;

  const Database &database_;
  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(SpecializePrimaryKeyIndexJoinAggregate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_SPECIALIZE_PRIMARY_KEY_INDEX_JOIN_AGGREGATE_HPP_
