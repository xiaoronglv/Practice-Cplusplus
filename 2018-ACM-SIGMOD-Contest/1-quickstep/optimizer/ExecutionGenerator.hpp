#ifndef PROJECT_OPTIMIZER_EXECUTION_GENERATOR_HPP_
#define PROJECT_OPTIMIZER_EXECUTION_GENERATOR_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/ChainedJoinAggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExecutionPlan.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/MultiwayEquiJoinAggregate.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/QueryHandle.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "storage/Attribute.hpp"
#include "storage/Database.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class ExecutionGenerator {
 public:
  ExecutionGenerator(Database *database, QueryHandle *query_handle);

  void generatePlan(const PlanPtr &plan);

 private:
  struct RelationInfo {
    RelationInfo(const std::size_t producer_operator_index_in,
                 const Relation *relation_in)
        : producer_operator_index(producer_operator_index_in),
          relation(relation_in) {}

    bool isStoredRelation() const {
      return producer_operator_index == kInvalidOperatorIndex;
    }

    const std::size_t producer_operator_index;
    const Relation *relation;

    static constexpr std::size_t kInvalidOperatorIndex = static_cast<std::size_t>(-1);
  };

  void generatePlanInternal(const PlanPtr &plan);

  void convertAggregate(const AggregatePtr &aggregate);
  void convertEquiJoin(const EquiJoinPtr &equi_join);
  void convertIndexScan(const SelectionPtr &index_scan);
  void convertPKIndexJoin(const EquiJoinPtr &index_join);
  void convertSelection(const SelectionPtr &selection);
  void convertTableReference(const TableReferencePtr &table_reference);
  void convertTableView(const TableViewPtr &table_view);

  void convertBuildSideFKIndexJoinAggregate(
      const EquiJoinAggregatePtr &index_join_aggregate);
  void convertFKPKScanJoinAggregate(
      const EquiJoinAggregatePtr &scan_join_aggregate);
  void convertChainedJoinAggregate(
      const ChainedJoinAggregatePtr &chained_join_aggregate);
  void convertEquiJoinAggregate(
      const EquiJoinAggregatePtr &equi_join_aggregate);
  void convertFKPKIndexJoinAggregate(
      const EquiJoinAggregatePtr &index_join_aggregate);
  void convertMultiwayEquiJoinAggregate(
      const MultiwayEquiJoinAggregatePtr &multiway_equi_join_aggregate);

  template <typename EquiJoinOperator>
  void convertEquiJoin(const EquiJoinPtr &equi_join);

  Relation* createTemporaryRelation(const PlanPtr &plan);
  void createDropTableOperator(const PlanPtr &plan);
  void createPrintSingleTupleOperator(const PlanPtr &plan,
                                      std::string *output_string);
  void createDependencyLink(const std::size_t producer_operator_index,
                            const std::size_t consumer_operator_index);

  const std::size_t query_id_;

  Database *database_;
  QueryHandle *query_handle_;
  ExecutionPlan *execution_plan_;

  std::unordered_map<ExprId, const Attribute*> attribute_substitution_map_;
  std::unordered_map<PlanPtr, RelationInfo> plan_to_output_relation_map_;
  std::vector<PlanPtr> temporary_relations_;

  const SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(ExecutionGenerator);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_EXECUTION_GENERATOR_HPP_
