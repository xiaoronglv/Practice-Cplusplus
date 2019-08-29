#ifndef PROJECT_OPTIMIZER_COST_SIMPLE_COST_MODEL_HPP_
#define PROJECT_OPTIMIZER_COST_SIMPLE_COST_MODEL_HPP_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ChainedJoinAggregate.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "storage/Attribute.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {

class Database;

namespace optimizer {

class SimpleCostModel {
 public:
  SimpleCostModel() {}

  std::size_t estimateCardinality(const PlanPtr &plan) const;

  std::size_t estimateNumDistinctValues(const ExprId expr_id,
                                        const PlanPtr &plan) const;

  double estimateSelectivity(const PlanPtr &plan) const;

  bool impliesUniqueAttributes(
      const PlanPtr &plan,
      const std::vector<AttributeReferencePtr> &attributes) const;

  bool impliesUniqueAttributes(const PlanPtr &plan,
                               const std::vector<ExprId> &attributes) const;

  const Attribute* findSourceAttribute(const ExprId expr_id,
                                       const PlanPtr &plan) const;

  bool isPrimaryKeyForeignKey(const Database &database,
                              const ExprId pk_id, const PlanPtr &pk_plan,
                              const ExprId fk_id, const PlanPtr &fk_plan) const;

  Range inferRange(const ExprId expr_id, const PlanPtr &plan) const;

  std::vector<Range> inferJoinAttributeRanges(
      const ChainedJoinAggregatePtr &chained_join_aggregate) const;

  std::uint32_t inferMaxCount(const ExprId expr_id, const PlanPtr &plan) const;

 private:
  std::size_t estimateCardinalityForEquiJoin(const EquiJoinPtr &plan) const;
  std::size_t estimateCardinalityForSelection(const SelectionPtr &plan) const;
  std::size_t estimateCardinalityForTableReference(const TableReferencePtr &plan) const;
  std::size_t estimateCardinalityForTableView(const TableViewPtr &plan) const;

  double estimateSelectivityForPredicate(const PredicatePtr &predicate,
                                         const PlanPtr &plan) const;
  double estimateSelectivityForComparison(const ComparisonPtr &comparison,
                                          const PlanPtr &plan) const;

  double estimateSelectivityForFilterPredicate(const PlanPtr &plan) const;

  std::size_t estimateNumDistinctValuesForTableReference(
      const ExprId expr_id,
      const TableReferencePtr &table_reference) const;

  double estimateDuplicationFactor(const ExprId expr_id,
                                   const PlanPtr &plan) const;

  Range inferRangeInternal(const ExprId expr_id,
                           const PlanPtr &plan,
                           const Range &input) const;

  Range inferRangeForEquiJoin(
      const ExprId expr_id,
      const std::vector<PlanPtr> &plans,
      const std::vector<const std::vector<AttributeReferencePtr>*> &join_attributes,
      const std::vector<PredicatePtr> &filter_predicates,
      const Range &input) const;

  attribute_id findCatalogRelationAttributeId(
      const TableReferencePtr &table_reference,
      const ExprId expr_id) const;

  static constexpr std::size_t kOne = static_cast<std::size_t>(1);

  DISALLOW_COPY_AND_ASSIGN(SimpleCostModel);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_COST_SIMPLE_COST_MODEL_HPP_

