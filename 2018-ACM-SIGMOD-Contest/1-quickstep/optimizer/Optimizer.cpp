#include "optimizer/Optimizer.hpp"

#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/AggregateFunction.hpp"
#include "optimizer/Alias.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/Conjunction.hpp"
#include "optimizer/ExecutionGenerator.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Filter.hpp"
#include "optimizer/MultiwayCartesianJoin.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Project.hpp"
#include "optimizer/QueryHandle.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/ScalarLiteral.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TopLevelPlan.hpp"
#include "optimizer/rules/CollapseSelection.hpp"
#include "optimizer/rules/ConstantPropagation.hpp"
#include "optimizer/rules/CreateTableViews.hpp"
#include "optimizer/rules/EliminatePredicateLiteral.hpp"
#include "optimizer/rules/FuseOperators.hpp"
#include "optimizer/rules/GenerateJoins.hpp"
#include "optimizer/rules/GenerateSelection.hpp"
#include "optimizer/rules/LiftUpExtraEquiJoinPredicate.hpp"
#include "optimizer/rules/PopulateFilterOnJoinAttributes.hpp"
#include "optimizer/rules/PruneColumns.hpp"
#include "optimizer/rules/PushDownFilter.hpp"
#include "optimizer/rules/ReducePredicate.hpp"
#include "optimizer/rules/ReduceSemiJoin.hpp"
#include "optimizer/rules/ReorderJoins.hpp"
#include "optimizer/rules/ResolveAlias.hpp"
#include "optimizer/rules/ReuseAggregateExpressions.hpp"
#include "optimizer/rules/Rule.hpp"
#include "optimizer/rules/SpecializeChainedJoinAggregate.hpp"
#include "optimizer/rules/SpecializeForeignKeyIndexJoinAggregate.hpp"
#include "optimizer/rules/SpecializeMultiwayJoinAggregate.hpp"
#include "optimizer/rules/SpecializePrimaryKeyIndexJoin.hpp"
#include "optimizer/rules/SpecializePrimaryKeyIndexJoinAggregate.hpp"
#include "optimizer/rules/SpecializePrimaryKeyScanJoinAggregate.hpp"
#include "optimizer/rules/SpecializeSelection.hpp"
#include "utility/ExecutionDAGVisualizer.hpp"
#include "utility/MemoryUtil.hpp"
#include "utility/PlanVisualizer.hpp"
#include "utility/StringUtil.hpp"
#include "utility/SyncStream.hpp"

#include "gflags/gflags.h"

namespace project {
namespace optimizer {

DEFINE_bool(enable_populate_filter_on_join_attributes, false,
            "Enable the PopulateFilterOnJoinAttributes rule");
DEFINE_bool(execute_query, true, "Whether to actually evaluate the queries");
DEFINE_bool(print_query, false, "Print query");
DEFINE_bool(print_plan, false, "Print query plan");
DEFINE_bool(visualize_plan, false, "Print query plan in DOT format");
DEFINE_bool(visualize_execution_dag, false, "Print execution plan in DOT format");

Optimizer::Optimizer(Database *database)
    : database_(database) {
}

void Optimizer::generateQueryHandle(const std::string &query,
                                    QueryHandle *query_handle) {
  const PlanPtr plan = optimize(parseQuery(query));

  if (FLAGS_print_query) {
    SyncStream(std::cerr)
        << "Query[" << query_handle->getQueryId() << "]\n" << query << "\n";
  }
  if (FLAGS_print_plan) {
    const std::string output = plan->toString();
    SyncStream(std::cerr)
        << "Plan[" << query_handle->getQueryId() << "]\n" << output;
  }
  if (FLAGS_visualize_plan) {
    PlanVisualizer visualizer;
    const std::string output = visualizer.visualize(plan);
    SyncStream(std::cerr)
        << "PlanGraph[" << query_handle->getQueryId() << "]\n" << output;
  }

  if (FLAGS_execute_query) {
    ExecutionGenerator execution_generator(database_, query_handle);
    execution_generator.generatePlan(plan);

    if (FLAGS_visualize_execution_dag) {
      ExecutionDAGVisualizer visualizer(*query_handle->getExecutionPlan());
      const std::string output = visualizer.toDOT();
      SyncStream(std::cerr)
          << "ExecutionGraph[" << query_handle->getQueryId() << "]\n" << output;
    }
  }
}

PlanPtr Optimizer::optimize(const PlanPtr &plan) {
  std::vector<std::unique_ptr<Rule<Plan>>> rules;

  if (FLAGS_enable_populate_filter_on_join_attributes) {
    rules.emplace_back(std::make_unique<PopulateFilterOnJoinAttributes>());
  }

  // Initial push-down of the filters.
  rules.emplace_back(std::make_unique<PushDownFilter>());

  // Convert cartesian products and filters to hash joins.
  rules.emplace_back(std::make_unique<GenerateJoins>(*database_));

  // Second round push-down of the filters.
  rules.emplace_back(std::make_unique<PushDownFilter>());

  // Fuse project and filter into selections.
  rules.emplace_back(std::make_unique<GenerateSelection>());
  rules.emplace_back(std::make_unique<PruneColumns>());
  rules.emplace_back(std::make_unique<CollapseSelection>());

  // Hash join order optimization.
  rules.emplace_back(std::make_unique<ReorderJoins>(*database_));
  rules.emplace_back(std::make_unique<PruneColumns>());
  rules.emplace_back(std::make_unique<CollapseSelection>());
  rules.emplace_back(std::make_unique<ResolveAlias>());
  rules.emplace_back(std::make_unique<PruneColumns>());

  // Reduce foreign-key primary-key semi joins.
  rules.emplace_back(std::make_unique<ReduceSemiJoin>(*database_));
  rules.emplace_back(std::make_unique<LiftUpExtraEquiJoinPredicate>());
  rules.emplace_back(std::make_unique<PruneColumns>());
  rules.emplace_back(std::make_unique<CollapseSelection>());

  // Constant propagation.
  rules.emplace_back(std::make_unique<ConstantPropagation>(&optimizer_context_));
  rules.emplace_back(std::make_unique<ReducePredicate>(&optimizer_context_));
  rules.emplace_back(std::make_unique<EliminatePredicateLiteral>(&optimizer_context_, database_));

  // Create table references.
  rules.emplace_back(std::make_unique<CreateTableViews>());

  // Extract common aggregate expressions.
  rules.emplace_back(std::make_unique<ReuseAggregateExpressions>());

  // Convert suitable selections into index scans.
  rules.emplace_back(std::make_unique<SpecializeSelection>());
  rules.emplace_back(std::make_unique<PruneColumns>());

  // Fuse aggregate and "chained" joins.
  rules.emplace_back(std::make_unique<SpecializeChainedJoinAggregate>());

  // Convert suitable joins into index joins that use primary key indexes.
  rules.emplace_back(std::make_unique<SpecializePrimaryKeyIndexJoin>(10000u));

  // Fuse aggregate and join that has the primary key constraint.
  rules.emplace_back(std::make_unique<SpecializePrimaryKeyScanJoinAggregate>());

  // Fuse aggregate and join that uses primary/foreign key indexes.
  // rules.emplace_back(std::make_unique<SpecializePrimaryKeyIndexJoinAggregate>(*database_));
  // rules.emplace_back(std::make_unique<SpecializeForeignKeyIndexJoinAggregate>());

  // Fuse aggregate and multiple joins that join on the same equivalent class
  // of attributes.
  rules.emplace_back(std::make_unique<SpecializeMultiwayJoinAggregate>());
  // TODO(robin-team): Fix EquiJoin case in the small dataset.
  // rules.emplace_back(std::make_unique<ResolveAlias>());

  // TODO(robin-team): Handle residual hash joins.
  rules.emplace_back(std::make_unique<SpecializePrimaryKeyIndexJoin>(1000000000u));

  // Fuse rest of the operators.
  rules.emplace_back(std::make_unique<FuseOperators>());

  // Apply rules.
  PlanPtr optimized_plan = plan;
  DVLOG(5) << "Init plan:\n" << plan->toString();
  for (const auto &rule : rules) {
    optimized_plan = rule->apply(optimized_plan);
    DVLOG(5) << "After applying rule " << rule->getName() << ":\n"
               << optimized_plan->toString();
  }
  return optimized_plan;
}

PlanPtr Optimizer::parseQuery(const std::string &query) {
  std::vector<std::string> components = SplitString(query, '|');
  CHECK_EQ(3u, components.size());

  std::vector<TableReferencePtr> tables;
  for (const std::string &relation : SplitString(components[0], ' ')) {
    tables.emplace_back(parseRelation(relation));
  }

  std::vector<PredicatePtr> predicates;
  for (const std::string &predicate : SplitString(components[1], '&')) {
    predicates.emplace_back(parsePredicate(predicate, tables));
  }

  std::vector<AttributeReferencePtr> project_attributes;
  for (const std::string &attribute : SplitString(components[2], ' ')) {
    project_attributes.emplace_back(parseAttribute(attribute, tables));
  }

  PlanPtr output;
  if (tables.size() == 1) {
    output = tables.front();
  } else {
    output = MultiwayCartesianJoin::Create(CastSharedPtrVector<Plan>(tables));
  }
  if (!predicates.empty()) {
    output = Filter::Create(output, CreateConjunctivePredicate(predicates));
  }
  output = Project::Create(output, project_attributes);

  std::vector<ScalarPtr> sum_expressions;
  for (const auto &attr : project_attributes) {
    const AggregateFunctionPtr aggregate =
        AggregateFunction::Create(AggregateFunctionType::kSum,
                                  std::static_pointer_cast<const Scalar>(attr));
    sum_expressions.emplace_back(Alias::Create(aggregate,
                                               optimizer_context_.nextExprId(),
                                               aggregate->toShortString()));
  }
  output = Aggregate::Create(output,
                             sum_expressions,
                             nullptr /* filter_predicate */);

  return TopLevelPlan::Create(output);
}

TableReferencePtr Optimizer::parseRelation(const std::string &relation) {
  return TableReference::Create(database_->getRelation(std::stoi(relation)),
                                &optimizer_context_);
}

PredicatePtr Optimizer::parsePredicate(
    const std::string &predicate,
    const std::vector<TableReferencePtr> &tables) {
  static const std::regex predicate_regex("(.*)([=<>])(.*)");
  std::smatch match;
  CHECK(std::regex_search(predicate, match, predicate_regex));

  const std::string op = match.str(2);
  DCHECK_EQ(1u, op.size());
  ComparisonType comparison_type;
  switch (op.front()) {
    case '=':
      comparison_type = ComparisonType::kEqual;
      break;
    case '<':
      comparison_type = ComparisonType::kLess;
      break;
    case '>':
      comparison_type = ComparisonType::kGreater;
      break;
    default:
      throw std::runtime_error("Unsupported comparison: " + op);
  }

  return Comparison::Create(comparison_type,
                            parseScalar(match.str(1), tables),
                            parseScalar(match.str(3), tables));
}

ScalarPtr Optimizer::parseScalar(
    const std::string &scalar,
    const std::vector<TableReferencePtr> &tables) {
  if (scalar.find('.') == std::string::npos) {
    return ScalarLiteral::Create(std::stoul(scalar),
                                 UInt64Type::Instance(),
                                 optimizer_context_.nextExprId());
  } else {
    return parseAttribute(scalar, tables);
  }
}

AttributeReferencePtr Optimizer::parseAttribute(
    const std::string &attribute,
    const std::vector<TableReferencePtr> &tables) const {
  std::vector<std::string> components = SplitString(attribute, '.');
  CHECK_EQ(2u, components.size());
  const std::size_t table_id = std::stoul(components[0]);
  const std::size_t attr_id = std::stoul(components[1]);
  return tables.at(table_id)->attribute_list().at(attr_id);
}

}  // namespace optimizer
}  // namespace project
