#include "optimizer/ExecutionGenerator.hpp"

#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/AggregateOperator.hpp"
#include "operators/ChainedJoinAggregateOperator.hpp"
#include "operators/CreateTableViewOperator.hpp"
#include "operators/DropTableOperator.hpp"
#include "operators/HashJoinOperator.hpp"
#include "operators/IndexJoinAggregateOperator.hpp"
#include "operators/IndexJoinOperator.hpp"
#include "operators/MultiwayJoinAggregateOperator.hpp"
#include "operators/OperatorTypedefs.hpp"
#include "operators/PrintOperator.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/ScanJoinAggregateOperator.hpp"
#include "operators/SelectOperator.hpp"
#include "operators/SortMergeJoinOperator.hpp"
#include "operators/expressions/Comparison.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "optimizer/Aggregate.hpp"
#include "optimizer/AggregateFunction.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/ChainedJoinAggregate.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/Conjunction.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/MultiwayEquiJoinAggregate.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/QueryHandle.hpp"
#include "optimizer/ScalarLiteral.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "optimizer/TopLevelPlan.hpp"
#include "storage/Attribute.hpp"
#include "storage/Database.hpp"
#include "storage/Relation.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

constexpr std::size_t ExecutionGenerator::RelationInfo::kInvalidOperatorIndex;

ExecutionGenerator::ExecutionGenerator(Database *database,
                                       QueryHandle *query_handle)
    : query_id_(query_handle->getQueryId()),
      database_(database),
      query_handle_(query_handle),
      execution_plan_(query_handle->getExecutionPlan()) {
}

void ExecutionGenerator::generatePlan(const PlanPtr &plan) {
  DCHECK(plan->getPlanType() == PlanType::kTopLevelPlan);

  const PlanPtr &target =
      std::static_pointer_cast<const TopLevelPlan>(plan)->plan();

  generatePlanInternal(target);
  createPrintSingleTupleOperator(target, query_handle_->getResultStringMutable());

  for (int i = temporary_relations_.size() - 1; i >= 0; --i) {
    createDropTableOperator(temporary_relations_.at(i));
  }
}

void ExecutionGenerator::generatePlanInternal(const PlanPtr &plan) {
  for (const PlanPtr &child : plan->children()) {
    generatePlanInternal(child);
  }

  switch (plan->getPlanType()) {
    case PlanType::kAggregate:
      return convertAggregate(
          std::static_pointer_cast<const Aggregate>(plan));
    case PlanType::kChainedJoinAggregate:
      return convertChainedJoinAggregate(
          std::static_pointer_cast<const ChainedJoinAggregate>(plan));
    case PlanType::kEquiJoin:
      return convertEquiJoin(
          std::static_pointer_cast<const EquiJoin>(plan));
    case PlanType::kEquiJoinAggregate:
      return convertEquiJoinAggregate(
          std::static_pointer_cast<const EquiJoinAggregate>(plan));
    case PlanType::kMultiwayEquiJoinAggregate:
      return convertMultiwayEquiJoinAggregate(
          std::static_pointer_cast<const MultiwayEquiJoinAggregate>(plan));
    case PlanType::kSelection:
      return convertSelection(
          std::static_pointer_cast<const Selection>(plan));
    case PlanType::kTableReference:
      return convertTableReference(
          std::static_pointer_cast<const TableReference>(plan));
    case PlanType::kTableView:
      return convertTableView(
          std::static_pointer_cast<const TableView>(plan));
    default:
      break;
  }
  LOG(FATAL) << "Unknown plan node " << plan->getShortString();
}

void ExecutionGenerator::convertAggregate(const AggregatePtr &aggregate) {
  const RelationInfo &input_relation_info =
      plan_to_output_relation_map_.at(aggregate->input());
  DCHECK(input_relation_info.relation != nullptr);
  const Relation &input_relation = *input_relation_info.relation;

  std::vector<std::unique_ptr<::project::Scalar>> aggregate_expressions;
  for (const auto &expr : aggregate->aggregate_expressions()) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(expr, &alias));

    AggregateFunctionPtr aggregate_function;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(),
                                                            &aggregate_function));

    DCHECK(aggregate_function->aggregate_type() == AggregateFunctionType::kSum);
    aggregate_expressions.emplace_back(
        aggregate_function->argument()->concretize(attribute_substitution_map_));
  }

  std::unique_ptr<project::Predicate> filter_predicate;
  if (aggregate->filter_predicate() != nullptr) {
    filter_predicate.reset(
        aggregate->filter_predicate()->concretize(attribute_substitution_map_));
  }

  Relation *output_relation = createTemporaryRelation(aggregate);

  const std::size_t aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new BasicAggregateOperator(query_id_,
                                     input_relation,
                                     std::move(aggregate_expressions),
                                     std::move(filter_predicate),
                                     output_relation));

  createDependencyLink(input_relation_info.producer_operator_index,
                       aggregate_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(aggregate),
      std::forward_as_tuple(aggregate_operator_index, output_relation));
}

void ExecutionGenerator::convertChainedJoinAggregate(
    const ChainedJoinAggregatePtr &chained_join_aggregate) {
  const auto &components = chained_join_aggregate->inputs();
  const auto &join_attribute_pairs = chained_join_aggregate->join_attribute_pairs();

  const std::size_t num_joins = join_attribute_pairs.size();
  DCHECK_EQ(num_joins + 1u, components.size());
  DCHECK_EQ(components.size(), chained_join_aggregate->filter_predicates().size());

  std::vector<const RelationInfo*> relation_infos;
  std::vector<const Relation*> relations;
  for (const auto &component : components) {
    const RelationInfo &info = plan_to_output_relation_map_.at(component);
    relation_infos.emplace_back(&info);

    DCHECK(info.relation != nullptr);
    relations.emplace_back(info.relation);
  }

  std::vector<std::pair<attribute_id, attribute_id>> join_attribute_id_pairs;
  for (std::size_t i = 0; i < join_attribute_pairs.size(); ++i) {
    const std::unique_ptr<::project::Scalar> build_scalar(
        join_attribute_pairs[i].first->concretize(attribute_substitution_map_));
    const std::unique_ptr<::project::Scalar> probe_scalar(
        join_attribute_pairs[i].second->concretize(attribute_substitution_map_));

    DCHECK(build_scalar->getScalarType() == ScalarType::kAttribute);
    DCHECK(probe_scalar->getScalarType() == ScalarType::kAttribute);

    join_attribute_id_pairs.emplace_back(
        static_cast<const ScalarAttribute&>(*build_scalar).attribute()->id(),
        static_cast<const ScalarAttribute&>(*probe_scalar).attribute()->id());
  }

  std::vector<std::unique_ptr<::project::Scalar>> aggregate_expressions;
  for (const auto &expr : chained_join_aggregate->aggregate_expressions()) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(expr, &alias));

    AggregateFunctionPtr aggregate_function;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(),
                                                            &aggregate_function));

    DCHECK(SubsetOfExpressions(aggregate_function->argument()
                                                 ->getReferencedAttributes(),
                               components.back()->getOutputAttributes()));

    DCHECK(aggregate_function->aggregate_type() == AggregateFunctionType::kSum);
    aggregate_expressions.emplace_back(
        aggregate_function->argument()->concretize(attribute_substitution_map_));
  }

  std::vector<std::unique_ptr<project::Predicate>> filter_predicates;
  for (const auto &filter_predicate : chained_join_aggregate->filter_predicates()) {
    if (filter_predicate == nullptr) {
      filter_predicates.emplace_back(nullptr);
      continue;
    }
    filter_predicates.emplace_back(
        filter_predicate->concretize(attribute_substitution_map_));
  }

  Relation *output_relation = createTemporaryRelation(chained_join_aggregate);

  const std::size_t chained_join_aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new ChainedJoinAggregateOperator(
              query_id_,
              relations,
              output_relation,
              join_attribute_id_pairs,
              cost_model_.inferJoinAttributeRanges(chained_join_aggregate),
              std::move(aggregate_expressions),
              std::move(filter_predicates)));

  for (const auto *relation_info : relation_infos) {
    createDependencyLink(relation_info->producer_operator_index,
                         chained_join_aggregate_operator_index);
  }

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(chained_join_aggregate),
      std::forward_as_tuple(chained_join_aggregate_operator_index,
                            output_relation));
}

void ExecutionGenerator::convertIndexScan(const SelectionPtr &index_scan) {
  DCHECK(index_scan->selection_type() == Selection::kIndexScan);

  const RelationInfo &input_relation_info =
      plan_to_output_relation_map_.at(index_scan->input());
  DCHECK(input_relation_info.relation != nullptr);
  const Relation &input_relation = *input_relation_info.relation;

  std::vector<std::unique_ptr<::project::Scalar>> project_expressions;
  for (const auto &expr : index_scan->project_expressions()) {
    project_expressions.emplace_back(expr->concretize(attribute_substitution_map_));
  }

  std::unique_ptr<project::Predicate> filter_predicate;
  DCHECK(index_scan->filter_predicate() != nullptr);
  filter_predicate.reset(
      index_scan->filter_predicate()->concretize(attribute_substitution_map_));

  DCHECK(filter_predicate->getPredicateType() == PredicateType::kComparison);
  std::unique_ptr<::project::Comparison> comparison(
      static_cast<::project::Comparison*>(filter_predicate.release()));

  Relation *output_relation = createTemporaryRelation(index_scan);

  const std::size_t index_scan_operator_index =
      execution_plan_->addRelationalOperator(
          new IndexScanOperator(query_id_,
                                input_relation,
                                output_relation,
                                std::move(project_expressions),
                                std::move(comparison)));

  createDependencyLink(input_relation_info.producer_operator_index,
                       index_scan_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(index_scan),
      std::forward_as_tuple(index_scan_operator_index, output_relation));
}

template <typename EquiJoinOperator>
void ExecutionGenerator::convertEquiJoin(const EquiJoinPtr &equi_join) {
  const RelationInfo &probe_relation_info =
      plan_to_output_relation_map_.at(equi_join->probe());
  DCHECK(probe_relation_info.relation != nullptr);
  const Relation &probe_relation = *probe_relation_info.relation;

  const RelationInfo &build_relation_info =
      plan_to_output_relation_map_.at(equi_join->build());
  DCHECK(build_relation_info.relation != nullptr);
  const Relation &build_relation = *build_relation_info.relation;

  DCHECK_EQ(1u, equi_join->probe_attributes().size());
  const std::unique_ptr<::project::Scalar> probe_attribubte(
      equi_join->probe_attributes().front()->concretize(attribute_substitution_map_));
  DCHECK(probe_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id probe_attribute_id =
      static_cast<const ScalarAttribute&>(*probe_attribubte).attribute()->id();

  DCHECK_EQ(1u, equi_join->build_attributes().size());
  const std::unique_ptr<::project::Scalar> build_attribubte(
      equi_join->build_attributes().front()->concretize(attribute_substitution_map_));
  DCHECK(build_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id build_attribute_id =
      static_cast<const ScalarAttribute&>(*build_attribubte).attribute()->id();

  const auto probe_output_attributes = equi_join->probe()->getOutputAttributes();
  const auto build_output_attributes = equi_join->build()->getOutputAttributes();

  std::vector<attribute_id> project_attributes;
  std::vector<JoinSide> project_attribute_sides;
  for (const auto &expr : equi_join->project_expressions()) {
    const std::unique_ptr<::project::Scalar> scalar(
        expr->concretize(attribute_substitution_map_));
    DCHECK(scalar->getScalarType() == ScalarType::kAttribute);

    project_attributes.emplace_back(
        static_cast<const ScalarAttribute&>(*scalar).attribute()->id());

    const ExprId expr_id = expr->getAttribute()->id();
    if (ContainsExprId(probe_output_attributes, expr_id)) {
      project_attribute_sides.emplace_back(JoinSide::kProbe);
    } else if (ContainsExprId(build_output_attributes, expr_id)) {
      project_attribute_sides.emplace_back(JoinSide::kBuild);
    } else {
      LOG(FATAL) << "Unsupport expression on EquiJoin that involves "
                 << "attributes from both sides";
    }
  }

  std::unique_ptr<project::Predicate> probe_filter_predicate;
  if (equi_join->probe_filter_predicate() != nullptr) {
    probe_filter_predicate.reset(
        equi_join->probe_filter_predicate()->concretize(attribute_substitution_map_));
  }

  std::unique_ptr<project::Predicate> build_filter_predicate;
  if (equi_join->build_filter_predicate() != nullptr) {
    build_filter_predicate.reset(
        equi_join->build_filter_predicate()->concretize(attribute_substitution_map_));
  }

  Relation *output_relation = createTemporaryRelation(equi_join);

  const std::size_t equi_join_aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new EquiJoinOperator(query_id_,
                               probe_relation,
                               build_relation,
                               output_relation,
                               probe_attribute_id,
                               build_attribute_id,
                               std::move(project_attributes),
                               std::move(project_attribute_sides),
                               std::move(probe_filter_predicate),
                               std::move(build_filter_predicate)));

  createDependencyLink(probe_relation_info.producer_operator_index,
                       equi_join_aggregate_operator_index);
  createDependencyLink(build_relation_info.producer_operator_index,
                       equi_join_aggregate_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(equi_join),
      std::forward_as_tuple(equi_join_aggregate_operator_index,
                            output_relation));
}

void ExecutionGenerator::convertEquiJoin(const EquiJoinPtr &equi_join) {
  switch (equi_join->join_type()) {
    case EquiJoin::kPrimaryKeyIndexJoin:
      convertPKIndexJoin(equi_join);
      break;;
    case EquiJoin::kHashInnerJoin:
      convertEquiJoin<BasicHashJoinOperator>(equi_join);
      break;
    case EquiJoin::kSortMergeJoin:
      convertEquiJoin<SortMergeJoinOperator>(equi_join);
      break;
    default:
      LOG(FATAL) << "Unsupported EquiJoin type";
  }
}

void ExecutionGenerator::convertEquiJoinAggregate(
    const EquiJoinAggregatePtr &equi_join_aggregate) {
  switch (equi_join_aggregate->join_type()) {
    case EquiJoinAggregate::kBuildSideForeignKeyIndex:
      return convertBuildSideFKIndexJoinAggregate(equi_join_aggregate);
    case EquiJoinAggregate::kForeignKeyIndexPrimaryKeyIndex:  // Fall through
    case EquiJoinAggregate::kForeignKeyPrimaryKeyIndex:
      return convertFKPKIndexJoinAggregate(equi_join_aggregate);
    case EquiJoinAggregate::kForeignKeyPrimaryKeyScan:
      return convertFKPKScanJoinAggregate(equi_join_aggregate);
  }
}

void ExecutionGenerator::convertMultiwayEquiJoinAggregate(
    const MultiwayEquiJoinAggregatePtr &multiway_join_aggregate) {
  const auto &components = multiway_join_aggregate->inputs();
  const auto &join_attributes = multiway_join_aggregate->join_attributes();

  const std::size_t num_components = components.size();
  DCHECK_EQ(num_components, join_attributes.size());
  DCHECK_EQ(num_components, multiway_join_aggregate->filter_predicates().size());

  std::vector<const RelationInfo*> relation_infos;
  std::vector<const Relation*> relations;
  for (const auto &component : components) {
    const RelationInfo &info = plan_to_output_relation_map_.at(component);
    relation_infos.emplace_back(&info);

    DCHECK(info.relation != nullptr);
    relations.emplace_back(info.relation);
  }

  std::vector<attribute_id> join_attribute_ids;
  std::vector<std::uint32_t> join_attribute_max_counts;

  for (std::size_t i = 0; i < num_components; ++i) {
    const std::unique_ptr<::project::Scalar> scalar(
        join_attributes[i]->concretize(attribute_substitution_map_));
    join_attribute_ids.emplace_back(
        static_cast<const ScalarAttribute&>(*scalar).attribute()->id());
    join_attribute_max_counts.emplace_back(
        cost_model_.inferMaxCount(join_attributes[i]->id(), components[i]));
  }

  std::vector<std::unique_ptr<::project::Scalar>> aggregate_expressions;
  std::vector<std::size_t> aggregate_expression_indexes;

  std::vector<std::vector<AttributeReferencePtr>> output_attributes;
  for (const auto &component : components) {
    output_attributes.emplace_back(component->getOutputAttributes());
  }

  for (const auto &expr : multiway_join_aggregate->aggregate_expressions()) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(expr, &alias));

    AggregateFunctionPtr aggregate_function;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(),
                                                            &aggregate_function));

    DCHECK(aggregate_function->aggregate_type() == AggregateFunctionType::kSum);
    const ScalarPtr argument = aggregate_function->argument();
    aggregate_expressions.emplace_back(
        argument->concretize(attribute_substitution_map_));

    const auto referenced_attributes = argument->getReferencedAttributes();
    int index = -1;
    for (std::size_t i = 0; i < num_components; ++i) {
      if (SubsetOfExpressions(referenced_attributes, output_attributes[i])) {
        index = i;
        break;
      }
    }

    if (index < 0) {
      LOG(FATAL) << "Unsupport expression on MultiwayEquiJoinAggregate that "
                 << "involves attributes from more that one components";
    }

    aggregate_expression_indexes.emplace_back(index);
  }

  std::vector<std::unique_ptr<project::Predicate>> filter_predicates;
  for (const auto &filter_predicate : multiway_join_aggregate->filter_predicates()) {
    if (filter_predicate == nullptr) {
      filter_predicates.emplace_back(nullptr);
      continue;
    }

    filter_predicates.emplace_back(
        filter_predicate->concretize(attribute_substitution_map_));
  }

  const Range join_attribute_range =
      cost_model_.inferRange(join_attributes.front()->id(), multiway_join_aggregate);

  Relation *output_relation = createTemporaryRelation(multiway_join_aggregate);

  const std::size_t multiway_join_aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new MultiwaySymmetricJoinAggregateOperator(
              query_id_,
              relations,
              output_relation,
              *database_,
              join_attribute_ids,
              join_attribute_max_counts,
              join_attribute_range,
              std::move(aggregate_expressions),
              std::move(aggregate_expression_indexes),
              std::move(filter_predicates)));

  for (const auto *relation_info : relation_infos) {
    createDependencyLink(relation_info->producer_operator_index,
                         multiway_join_aggregate_operator_index);
  }

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(multiway_join_aggregate),
      std::forward_as_tuple(multiway_join_aggregate_operator_index,
                            output_relation));
}

void ExecutionGenerator::convertFKPKIndexJoinAggregate(
    const EquiJoinAggregatePtr &index_join_aggregate) {
  const bool use_foreign_key_index =
      index_join_aggregate->join_type() ==
          EquiJoinAggregate::kForeignKeyIndexPrimaryKeyIndex;

  const RelationInfo &probe_relation_info =
      plan_to_output_relation_map_.at(index_join_aggregate->probe());
  DCHECK(probe_relation_info.relation != nullptr);
  const Relation &probe_relation = *probe_relation_info.relation;

  const RelationInfo &build_relation_info =
      plan_to_output_relation_map_.at(index_join_aggregate->build());
  DCHECK(build_relation_info.relation != nullptr);
  const Relation &build_relation = *build_relation_info.relation;

  DCHECK_EQ(1u, index_join_aggregate->probe_attributes().size());
  const std::unique_ptr<::project::Scalar> probe_attribubte(
      index_join_aggregate->probe_attributes().front()
                          ->concretize(attribute_substitution_map_));
  DCHECK(probe_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id probe_attribute_id =
      static_cast<const ScalarAttribute&>(*probe_attribubte).attribute()->id();

  DCHECK_EQ(1u, index_join_aggregate->build_attributes().size());
  const std::unique_ptr<::project::Scalar> build_attribubte(
      index_join_aggregate->build_attributes().front()
                          ->concretize(attribute_substitution_map_));
  DCHECK(build_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id build_attribute_id =
      static_cast<const ScalarAttribute&>(*build_attribubte).attribute()->id();

  const auto probe_output_attributes =
      index_join_aggregate->probe()->getOutputAttributes();
  const auto build_output_attributes =
      index_join_aggregate->build()->getOutputAttributes();

  std::vector<std::unique_ptr<::project::Scalar>> aggregate_expressions;
  std::vector<JoinSide> aggregate_expression_sides;
  for (const auto &expr : index_join_aggregate->aggregate_expressions()) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(expr, &alias));

    AggregateFunctionPtr aggregate_function;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(),
                                                            &aggregate_function));

    DCHECK(aggregate_function->aggregate_type() == AggregateFunctionType::kSum);
    const ScalarPtr argument = aggregate_function->argument();
    aggregate_expressions.emplace_back(
        argument->concretize(attribute_substitution_map_));

    const auto referenced_attributes = argument->getReferencedAttributes();
    if (SubsetOfExpressions(referenced_attributes, probe_output_attributes)) {
      aggregate_expression_sides.emplace_back(JoinSide::kProbe);
    } else if (SubsetOfExpressions(referenced_attributes, build_output_attributes)) {
      aggregate_expression_sides.emplace_back(JoinSide::kBuild);
    } else {
      LOG(FATAL) << "Unsupport expression on PKIndexJoinAggregate that "
                 << "involves attributes from both sides";
    }
  }

  std::unique_ptr<project::Predicate> probe_filter_predicate;
  if (index_join_aggregate->probe_filter_predicate() != nullptr) {
    probe_filter_predicate.reset(
        index_join_aggregate->probe_filter_predicate()
                            ->concretize(attribute_substitution_map_));
  }
  DCHECK(index_join_aggregate->build_filter_predicate() == nullptr);

  Relation *output_relation = createTemporaryRelation(index_join_aggregate);

  const std::size_t fk_pk_index_join_aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new FKPKIndexJoinAggregateOperator(
              query_id_,
              probe_relation,
              build_relation,
              output_relation,
              probe_attribute_id,
              build_attribute_id,
              std::move(aggregate_expressions),
              std::move(aggregate_expression_sides),
              std::move(probe_filter_predicate),
              use_foreign_key_index));

  createDependencyLink(probe_relation_info.producer_operator_index,
                       fk_pk_index_join_aggregate_operator_index);
  createDependencyLink(build_relation_info.producer_operator_index,
                       fk_pk_index_join_aggregate_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(index_join_aggregate),
      std::forward_as_tuple(fk_pk_index_join_aggregate_operator_index,
                            output_relation));
}

void ExecutionGenerator::convertBuildSideFKIndexJoinAggregate(
    const EquiJoinAggregatePtr &index_join_aggregate) {
  const RelationInfo &probe_relation_info =
      plan_to_output_relation_map_.at(index_join_aggregate->probe());
  DCHECK(probe_relation_info.relation != nullptr);
  const Relation &probe_relation = *probe_relation_info.relation;

  const RelationInfo &build_relation_info =
      plan_to_output_relation_map_.at(index_join_aggregate->build());
  DCHECK(build_relation_info.relation != nullptr);
  const Relation &build_relation = *build_relation_info.relation;

  DCHECK_EQ(1u, index_join_aggregate->probe_attributes().size());
  const std::unique_ptr<::project::Scalar> probe_attribubte(
      index_join_aggregate->probe_attributes().front()
                          ->concretize(attribute_substitution_map_));
  DCHECK(probe_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id probe_attribute_id =
      static_cast<const ScalarAttribute&>(*probe_attribubte).attribute()->id();

  DCHECK_EQ(1u, index_join_aggregate->build_attributes().size());
  const std::unique_ptr<::project::Scalar> build_attribubte(
      index_join_aggregate->build_attributes().front()
                          ->concretize(attribute_substitution_map_));
  DCHECK(build_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id build_attribute_id =
      static_cast<const ScalarAttribute&>(*build_attribubte).attribute()->id();

  std::vector<std::unique_ptr<::project::Scalar>> aggregate_expressions;
  for (const auto &expr : index_join_aggregate->aggregate_expressions()) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(expr, &alias));

    AggregateFunctionPtr aggregate_function;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(),
                                                            &aggregate_function));

    DCHECK(aggregate_function->aggregate_type() == AggregateFunctionType::kSum);
    const ScalarPtr argument = aggregate_function->argument();
    aggregate_expressions.emplace_back(
        argument->concretize(attribute_substitution_map_));

    DCHECK(SubsetOfExpressions(argument->getReferencedAttributes(),
                               index_join_aggregate->probe()->getOutputAttributes()));
  }

  std::unique_ptr<project::Predicate> probe_filter_predicate;
  if (index_join_aggregate->probe_filter_predicate() != nullptr) {
    probe_filter_predicate.reset(
        index_join_aggregate->probe_filter_predicate()
                            ->concretize(attribute_substitution_map_));
  }

  std::unique_ptr<project::Predicate> build_filter_predicate;
  if (index_join_aggregate->build_filter_predicate() != nullptr) {
    build_filter_predicate.reset(
        index_join_aggregate->build_filter_predicate()
                            ->concretize(attribute_substitution_map_));
  }

  Relation *output_relation = createTemporaryRelation(index_join_aggregate);

  const std::size_t build_side_fk_index_join_aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new BuildSideFKIndexJoinAggregateOperator(
              query_id_,
              probe_relation,
              build_relation,
              output_relation,
              probe_attribute_id,
              build_attribute_id,
              std::move(aggregate_expressions),
              std::move(probe_filter_predicate),
              std::move(build_filter_predicate)));

  createDependencyLink(probe_relation_info.producer_operator_index,
                       build_side_fk_index_join_aggregate_operator_index);
  createDependencyLink(build_relation_info.producer_operator_index,
                       build_side_fk_index_join_aggregate_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(index_join_aggregate),
      std::forward_as_tuple(build_side_fk_index_join_aggregate_operator_index,
                            output_relation));
}

void ExecutionGenerator::convertFKPKScanJoinAggregate(
    const EquiJoinAggregatePtr &scan_join_aggregate) {
  const RelationInfo &probe_relation_info =
      plan_to_output_relation_map_.at(scan_join_aggregate->probe());
  DCHECK(probe_relation_info.relation != nullptr);
  const Relation &probe_relation = *probe_relation_info.relation;

  const RelationInfo &build_relation_info =
      plan_to_output_relation_map_.at(scan_join_aggregate->build());
  DCHECK(build_relation_info.relation != nullptr);
  const Relation &build_relation = *build_relation_info.relation;

  DCHECK_EQ(1u, scan_join_aggregate->probe_attributes().size());
  const std::unique_ptr<::project::Scalar> probe_attribubte(
      scan_join_aggregate->probe_attributes().front()
                          ->concretize(attribute_substitution_map_));
  DCHECK(probe_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id probe_attribute_id =
      static_cast<const ScalarAttribute&>(*probe_attribubte).attribute()->id();

  DCHECK_EQ(1u, scan_join_aggregate->build_attributes().size());
  const std::unique_ptr<::project::Scalar> build_attribubte(
      scan_join_aggregate->build_attributes().front()
                          ->concretize(attribute_substitution_map_));
  DCHECK(build_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id build_attribute_id =
      static_cast<const ScalarAttribute&>(*build_attribubte).attribute()->id();

  const auto probe_output_attributes =
      scan_join_aggregate->probe()->getOutputAttributes();
  const auto build_output_attributes =
      scan_join_aggregate->build()->getOutputAttributes();

  std::vector<std::unique_ptr<::project::Scalar>> aggregate_expressions;
  std::vector<JoinSide> aggregate_expression_sides;
  for (const auto &expr : scan_join_aggregate->aggregate_expressions()) {
    AliasPtr alias;
    CHECK(SomeAlias::MatchesWithConditionalCast(expr, &alias));

    AggregateFunctionPtr aggregate_function;
    CHECK(SomeAggregateFunction::MatchesWithConditionalCast(alias->expression(),
                                                            &aggregate_function));

    DCHECK(aggregate_function->aggregate_type() == AggregateFunctionType::kSum);
    const ScalarPtr argument = aggregate_function->argument();
    aggregate_expressions.emplace_back(
        argument->concretize(attribute_substitution_map_));

    const auto referenced_attributes = argument->getReferencedAttributes();
    if (SubsetOfExpressions(referenced_attributes, probe_output_attributes)) {
      aggregate_expression_sides.emplace_back(JoinSide::kProbe);
    } else if (SubsetOfExpressions(referenced_attributes, build_output_attributes)) {
      aggregate_expression_sides.emplace_back(JoinSide::kBuild);
    } else {
      LOG(FATAL) << "Unsupport expression on FKPKScanJoinAggregate that "
                 << "involves attributes from both sides";
    }
  }

  std::unique_ptr<project::Predicate> probe_filter_predicate;
  if (scan_join_aggregate->probe_filter_predicate()) {
    probe_filter_predicate.reset(
        scan_join_aggregate->probe_filter_predicate()
                           ->concretize(attribute_substitution_map_));
  }

  DCHECK(scan_join_aggregate->build_filter_predicate());
  std::unique_ptr<project::Predicate> build_filter_predicate(
      scan_join_aggregate->build_filter_predicate()
                         ->concretize(attribute_substitution_map_));

  Relation *output_relation = createTemporaryRelation(scan_join_aggregate);

  const std::size_t fk_pk_scan_join_aggregate_operator_index =
      execution_plan_->addRelationalOperator(
          new FKPKScanJoinAggregateOperator(
              query_id_,
              probe_relation,
              build_relation,
              output_relation,
              probe_attribute_id,
              build_attribute_id,
              std::move(aggregate_expressions),
              std::move(aggregate_expression_sides),
              std::move(probe_filter_predicate),
              std::move(build_filter_predicate)));

  createDependencyLink(probe_relation_info.producer_operator_index,
                       fk_pk_scan_join_aggregate_operator_index);
  createDependencyLink(build_relation_info.producer_operator_index,
                       fk_pk_scan_join_aggregate_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(scan_join_aggregate),
      std::forward_as_tuple(fk_pk_scan_join_aggregate_operator_index,
                            output_relation));
}

void ExecutionGenerator::convertPKIndexJoin(const EquiJoinPtr &index_join) {
  const RelationInfo &probe_relation_info =
      plan_to_output_relation_map_.at(index_join->probe());
  DCHECK(probe_relation_info.relation != nullptr);
  const Relation &probe_relation = *probe_relation_info.relation;

  const RelationInfo &build_relation_info =
      plan_to_output_relation_map_.at(index_join->build());
  DCHECK(build_relation_info.relation != nullptr);
  const Relation &build_relation = *build_relation_info.relation;

  DCHECK_EQ(1u, index_join->probe_attributes().size());
  const std::unique_ptr<::project::Scalar> probe_attribubte(
      index_join->probe_attributes().front()->concretize(attribute_substitution_map_));
  DCHECK(probe_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id probe_attribute_id =
      static_cast<const ScalarAttribute&>(*probe_attribubte).attribute()->id();

  DCHECK_EQ(1u, index_join->build_attributes().size());
  const std::unique_ptr<::project::Scalar> build_attribubte(
      index_join->build_attributes().front()->concretize(attribute_substitution_map_));
  DCHECK(build_attribubte->getScalarType() == ScalarType::kAttribute);
  const attribute_id build_attribute_id =
      static_cast<const ScalarAttribute&>(*build_attribubte).attribute()->id();

  const auto probe_output_attributes = index_join->probe()->getOutputAttributes();
  const auto build_output_attributes = index_join->build()->getOutputAttributes();

  std::vector<std::unique_ptr<::project::Scalar>> project_expressions;
  std::vector<JoinSide> project_expression_sides;
  for (const auto &expr : index_join->project_expressions()) {
    project_expressions.emplace_back(expr->concretize(attribute_substitution_map_));

    const auto expr_attributes = expr->getReferencedAttributes();
    if (SubsetOfExpressions(expr_attributes, probe_output_attributes)) {
      project_expression_sides.emplace_back(JoinSide::kProbe);
    } else if (SubsetOfExpressions(expr_attributes, build_output_attributes)) {
      project_expression_sides.emplace_back(JoinSide::kBuild);
    } else {
      LOG(FATAL) << "Unsupport expression on PkIndexLookupJoin that involves "
                 << "attributes from both sides";
    }
  }

  std::unique_ptr<project::Predicate> probe_filter_predicate;
  if (index_join->probe_filter_predicate() != nullptr) {
    probe_filter_predicate.reset(
        index_join->probe_filter_predicate()->concretize(attribute_substitution_map_));
  }
  DCHECK(index_join->build_filter_predicate() == nullptr);

  Relation *output_relation = createTemporaryRelation(index_join);

  const std::size_t pk_index_join_operator_index =
      execution_plan_->addRelationalOperator(
          new PrimaryKeyIndexJoinOperator(
              query_id_,
              probe_relation,
              build_relation,
              output_relation,
              probe_attribute_id,
              build_attribute_id,
              std::move(project_expressions),
              std::move(project_expression_sides),
              std::move(probe_filter_predicate)));

  createDependencyLink(probe_relation_info.producer_operator_index,
                       pk_index_join_operator_index);
  createDependencyLink(build_relation_info.producer_operator_index,
                       pk_index_join_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(index_join),
      std::forward_as_tuple(pk_index_join_operator_index, output_relation));
}

void ExecutionGenerator::convertSelection(const SelectionPtr &selection) {
  switch (selection->selection_type()) {
    case Selection::kIndexScan:
      convertIndexScan(selection);
      return;
    default:
      break;
  }
  DCHECK(selection->selection_type() == Selection::kBasic);

  const RelationInfo &input_relation_info =
      plan_to_output_relation_map_.at(selection->input());
  DCHECK(input_relation_info.relation != nullptr);
  const Relation &input_relation = *input_relation_info.relation;

  std::vector<std::unique_ptr<::project::Scalar>> project_expressions;
  for (const auto &expr : selection->project_expressions()) {
    project_expressions.emplace_back(expr->concretize(attribute_substitution_map_));
  }

  std::unique_ptr<project::Predicate> filter_predicate;
  if (selection->filter_predicate() != nullptr) {
    filter_predicate.reset(
        selection->filter_predicate()->concretize(attribute_substitution_map_));
  }

  Relation *output_relation = createTemporaryRelation(selection);

  const std::size_t select_operator_index =
      execution_plan_->addRelationalOperator(
          new BasicSelectOperator(query_id_,
                                  input_relation,
                                  output_relation,
                                  std::move(project_expressions),
                                  std::move(filter_predicate)));

  createDependencyLink(input_relation_info.producer_operator_index,
                       select_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(selection),
      std::forward_as_tuple(select_operator_index, output_relation));
}

void ExecutionGenerator::convertTableReference(
    const TableReferencePtr &table_reference) {
  const Relation &relation = table_reference->relation();
  const auto &attribute_list = table_reference->attribute_list();

  for (std::size_t i = 0; i < relation.getNumAttributes(); ++i) {
    attribute_substitution_map_.emplace(attribute_list[i]->id(),
                                        &relation.getAttribute(i));
  }

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(table_reference),
      std::forward_as_tuple(RelationInfo::kInvalidOperatorIndex,
                            &relation));
}

void ExecutionGenerator::convertTableView(const TableViewPtr &table_view) {
  const RelationInfo &input_relation_info =
      plan_to_output_relation_map_.at(table_view->input());
  DCHECK(input_relation_info.relation != nullptr);
  const Relation &input_relation = *input_relation_info.relation;

  DCHECK(table_view->comparison() != nullptr);
  std::unique_ptr<::project::Predicate> predicate(
      table_view->comparison()->concretize(attribute_substitution_map_));
  DCHECK(predicate->getPredicateType() == PredicateType::kComparison);
  std::unique_ptr<::project::Comparison> comparison(
      static_cast<::project::Comparison*>(predicate.release()));

  Relation *output_relation = createTemporaryRelation(table_view);

  const std::size_t create_table_view_operator_index =
      execution_plan_->addRelationalOperator(
          new CreateTableViewOperator(query_id_,
                                      input_relation,
                                      output_relation,
                                      std::move(comparison),
                                      table_view->sort_order()));

  createDependencyLink(input_relation_info.producer_operator_index,
                       create_table_view_operator_index);

  plan_to_output_relation_map_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(table_view),
      std::forward_as_tuple(create_table_view_operator_index, output_relation));
}

Relation* ExecutionGenerator::createTemporaryRelation(const PlanPtr &plan) {
  const auto attributes = plan->getOutputAttributes();
  const std::size_t num_attributes = attributes.size();

  std::vector<const Type*> attribute_types;
  attribute_types.reserve(num_attributes);
  for (const auto &attr : attributes) {
    attribute_types.emplace_back(&attr->getValueType());
  }

  const relation_id rel_id = database_->addRelation(
      new Relation(attribute_types, true /* is_temporary */));

  Relation *relation = database_->getRelationMutable(rel_id);
  for (std::size_t i = 0; i < num_attributes; ++i) {
    attribute_substitution_map_[attributes[i]->id()] = &relation->getAttribute(i);
  }

  temporary_relations_.emplace_back(plan);
  return relation;
}

void ExecutionGenerator::createDropTableOperator(const PlanPtr &plan) {
  const auto &info = plan_to_output_relation_map_.at(plan);
  DCHECK(!info.isStoredRelation());

  const std::size_t drop_table_operator_index =
      execution_plan_->addRelationalOperator(
          new DropTableOperator(query_id_, *info.relation, database_));

  const auto &consumers =
      execution_plan_->getDependents(info.producer_operator_index);

  if (consumers.empty()) {
    execution_plan_->createLink(info.producer_operator_index,
                                drop_table_operator_index);
  } else {
    for (const std::size_t consumer_operator_index : consumers) {
      execution_plan_->createLink(consumer_operator_index,
                                  drop_table_operator_index);
    }
  }
}

void ExecutionGenerator::createPrintSingleTupleOperator(
    const PlanPtr &plan, std::string *output_string) {
  const auto it = plan_to_output_relation_map_.find(plan);
  DCHECK(it != plan_to_output_relation_map_.end());

  const std::size_t print_single_tuple_operator_index =
      execution_plan_->addRelationalOperator(
          new PrintSingleTupleToStringOperator(query_id_,
                                               *it->second.relation,
                                               output_string));

  createDependencyLink(it->second.producer_operator_index,
                       print_single_tuple_operator_index);
}

void ExecutionGenerator::createDependencyLink(
    const std::size_t producer_operator_index,
    const std::size_t consumer_operator_index) {
  if (producer_operator_index != RelationInfo::kInvalidOperatorIndex &&
      consumer_operator_index != RelationInfo::kInvalidOperatorIndex) {
    execution_plan_->createLink(producer_operator_index,
                                consumer_operator_index);
  }
}

}  // namespace optimizer
}  // namespace project
