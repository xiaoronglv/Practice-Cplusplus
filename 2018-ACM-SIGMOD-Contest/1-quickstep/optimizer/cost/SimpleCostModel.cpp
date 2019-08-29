#include "optimizer/cost/SimpleCostModel.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "optimizer/ChainedJoinAggregate.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/Conjunction.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/MultiwayEquiJoinAggregate.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/PredicateLiteral.hpp"
#include "optimizer/ScalarLiteral.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "optimizer/TopLevelPlan.hpp"
#include "storage/Attribute.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Database.hpp"
#include "storage/Relation.hpp"
#include "storage/RelationStatistics.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

constexpr std::size_t SimpleCostModel::kOne;

std::size_t SimpleCostModel::estimateCardinality(const PlanPtr &plan) const {
  std::size_t cardinality = 0;
  switch (plan->getPlanType()) {
    case PlanType::kAggregate:  // Fall through
    case PlanType::kChainedJoinAggregate:
    case PlanType::kEquiJoinAggregate:
    case PlanType::kMultiwayEquiJoinAggregate:
      return 1u;
    case PlanType::kEquiJoin:
      cardinality = estimateCardinalityForEquiJoin(
          std::static_pointer_cast<const EquiJoin>(plan));
      break;
    case PlanType::kSelection:
      cardinality = estimateCardinalityForSelection(
          std::static_pointer_cast<const Selection>(plan));
      break;
    case PlanType::kTableReference:
      cardinality = estimateCardinalityForTableReference(
          std::static_pointer_cast<const TableReference>(plan));
      break;
    case PlanType::kTableView:
      cardinality = estimateCardinalityForTableView(
          std::static_pointer_cast<const TableView>(plan));
      break;
    case PlanType::kTopLevelPlan:
      cardinality = estimateCardinality(
          std::static_pointer_cast<const TopLevelPlan>(plan)->plan());
      break;
    default:
      LOG(FATAL) << "Unsupport plan in SimpleCostModel::estimateCardinality()\n"
                 << plan->toString();
  }
  return std::max(cardinality, kOne);
}

std::size_t SimpleCostModel::estimateCardinalityForEquiJoin(
    const EquiJoinPtr &plan) const {
  const std::size_t probe_cardinality = estimateCardinality(plan->probe());
  const std::size_t build_cardinality = estimateCardinality(plan->build());

  const double probe_selectivity = estimateSelectivity(plan->probe());
  const double build_selectivity = estimateSelectivity(plan->build());

  double probe_duplication_factor = 1;
  double build_duplication_factor = 1;
  if (plan->probe_attributes().size() == 1) {
    probe_duplication_factor =
        estimateDuplicationFactor(plan->probe_attributes().front()->id(), plan->probe());
    build_duplication_factor =
        estimateDuplicationFactor(plan->build_attributes().front()->id(), plan->build());
  }

  const double filter_selectivity =
      estimateSelectivityForPredicate(plan->probe_filter_predicate(), plan) *
      estimateSelectivityForPredicate(plan->build_filter_predicate(), plan);

  const double duplication_factor = std::min(probe_duplication_factor,
                                             build_duplication_factor);

  return std::max(
      probe_cardinality * build_selectivity * filter_selectivity * duplication_factor,
      build_cardinality * probe_selectivity * filter_selectivity * duplication_factor);
}

std::size_t SimpleCostModel::estimateCardinalityForSelection(
    const SelectionPtr &plan) const {
  const double selectivity =
      estimateSelectivityForPredicate(plan->filter_predicate(), plan);
  return estimateCardinality(plan->input()) * selectivity;
}

std::size_t SimpleCostModel::estimateCardinalityForTableReference(
    const TableReferencePtr &plan) const {
  return plan->relation().getStatistics().getNumTuples();
}

std::size_t SimpleCostModel::estimateCardinalityForTableView(
    const TableViewPtr &plan) const {
  const double selectivity =
      estimateSelectivityForPredicate(plan->comparison(), plan);
  return estimateCardinality(plan->input()) * selectivity;
}

double SimpleCostModel::estimateSelectivity(const PlanPtr &plan) const {
  switch (plan->getPlanType()) {
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(plan);
      const double probe_selectivity = estimateSelectivity(equi_join->probe());
      const double build_selectivity = estimateSelectivity(equi_join->build());
      const double filter_selectivity =
          estimateSelectivityForPredicate(equi_join->probe_filter_predicate(),
                                          equi_join->probe()) *
          estimateSelectivityForPredicate(equi_join->build_filter_predicate(),
                                          equi_join->build());
      return std::min(probe_selectivity, build_selectivity) * filter_selectivity;
    }
    case PlanType::kSelection:  // Fall through
    case PlanType::kTableView: {
      const double filter_selectivity =
          estimateSelectivityForFilterPredicate(plan);
      const double input_selectivity =
          estimateSelectivity(plan->children().front());
      return std::min(filter_selectivity, input_selectivity);
    }
    default:
      break;
  }

  if (plan->getNumChildren() == 1) {
    return estimateSelectivity(plan->children().front());
  }

  return 1.0;
}

double SimpleCostModel::estimateSelectivityForFilterPredicate(
    const PlanPtr &plan) const {
  std::vector<PlanPtr> nodes;
  std::vector<PredicatePtr> filter_predicates;
  switch (plan->getPlanType()) {
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(plan);
      nodes.emplace_back(selection->input());
      filter_predicates.emplace_back(selection->filter_predicate());
      break;
    }
    case PlanType::kTableView: {
      const TableViewPtr &table_view =
          std::static_pointer_cast<const TableView>(plan);
      nodes.emplace_back(table_view->input());
      filter_predicates.emplace_back(table_view->comparison());
      break;
    }
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(plan);
      nodes.emplace_back(equi_join->probe());
      filter_predicates.emplace_back(equi_join->probe_filter_predicate());
      nodes.emplace_back(equi_join->build());
      filter_predicates.emplace_back(equi_join->build_filter_predicate());
    }
    default:
      break;
  }

  double selectivity = 1;
  for (const auto &filter_predicate : filter_predicates) {
    selectivity *= estimateSelectivityForPredicate(filter_predicate, plan);
  }
  return selectivity;
}

double SimpleCostModel::estimateSelectivityForPredicate(
    const PredicatePtr &predicate,
    const PlanPtr &plan) const {
  if (predicate == nullptr) {
    return 1.0;
  }

  switch (predicate->getExpressionType()) {
    case ExpressionType::kComparison: {
      const ComparisonPtr &comparison =
          std::static_pointer_cast<const Comparison>(predicate);
      return estimateSelectivityForComparison(comparison, plan);
    }
    case ExpressionType::kConjunction: {
      const ConjunctionPtr &conjunction =
          std::static_pointer_cast<const Conjunction>(predicate);
      double selectivity = 1.0;
      for (const auto &component : conjunction->operands()) {
        selectivity = std::min(selectivity,
                               estimateSelectivityForPredicate(component, plan));
      }
      return selectivity;
    }
    case ExpressionType::kPredicateLiteral: {
      const PredicateLiteralPtr &literal =
          std::static_pointer_cast<const PredicateLiteral>(predicate);
      return literal->value() ? 1.0 : 0;
    }
    default:
      break;
  }
  return 0.5;
}

double SimpleCostModel::estimateSelectivityForComparison(
    const ComparisonPtr &comparison,
    const PlanPtr &plan) const {
  // Case 1 - Compare with literal
  //   Case 1.1 - Equality comparison
  //     Case 1.1.1 - Statistics available: 1.0 / num_distinct_values
  //     Case 1.2.2 - Otherwise: 0.1
  //   Case 1.2 - Inequality comparison
  //     Case 1.2.1 - Statistics available: delta / range
  //     Case 1.2.2 - Otherwise: 0.5
  // Case 2 - Otherwise: 0.1

  AttributeReferencePtr attr;
  ScalarLiteralPtr literal;

  if (!(SomeScalarLiteral::MatchesWithConditionalCast(comparison->left(), &literal) &&
        SomeAttributeReference::MatchesWithConditionalCast(comparison->right(), &attr)) &&
      !(SomeScalarLiteral::MatchesWithConditionalCast(comparison->right(), &literal) &&
        SomeAttributeReference::MatchesWithConditionalCast(comparison->left(), &attr))) {
    return 0.1;
  }

  if (comparison->comparison_type() == ComparisonType::kEqual) {
    for (const auto &child : plan->children()) {
      if (ContainsExprId(child->getOutputAttributes(), attr->id())) {
        return 1.0 / estimateNumDistinctValues(attr->id(), child);
      }
    }
    return 0.1;
  }

  Range range = Range::Max();

  for (const auto &child : plan->children()) {
    if (ContainsExprId(child->getOutputAttributes(), attr->id())) {
      range = inferRange(attr->id(), child);
      break;
    }
  }

  if (!range.valid()) {
    return 0.5;
  }
  const std::uint64_t min_value = range.begin();
  const std::uint64_t max_value = range.end();
  DCHECK_LE(min_value, max_value);

  const bool use_lower_range =
      SomeAttributeReference::Matches(comparison->left())
          ^ (comparison->comparison_type() == ComparisonType::kGreater);

  const std::uint64_t value = literal->value();
  double delta = use_lower_range ? value - min_value : max_value - value;
  delta = std::max(delta, static_cast<double>(0));

  return delta / (max_value - min_value + 1);
}

std::size_t SimpleCostModel::estimateNumDistinctValues(
    const ExprId expr_id, const PlanPtr &plan) const {
  TableReferencePtr table_reference;
  if (SomeTableReference::MatchesWithConditionalCast(plan, &table_reference)) {
    return estimateNumDistinctValuesForTableReference(expr_id, table_reference);
  }

  switch (plan->getPlanType()) {
    case PlanType::kAggregate:
      return 1u;
    case PlanType::kSelection:  // Fall through
    case PlanType::kTableView: {
      const double filter_selectivity =
          estimateSelectivityForFilterPredicate(plan);
      const PlanPtr &child = plan->children().front();
      if (ContainsExprId(child->getOutputAttributes(), expr_id)) {
        const std::size_t estimated =
            estimateNumDistinctValues(expr_id, child) * filter_selectivity;
        return std::max(estimated, kOne);
      }
      break;
    }
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(plan);

      const double filter_selectivity =
          estimateSelectivityForPredicate(equi_join->probe_filter_predicate(),
                                          equi_join->probe()) *
          estimateSelectivityForPredicate(equi_join->build_filter_predicate(),
                                          equi_join->build());

      if (ContainsExprId(equi_join->probe()->getOutputAttributes(), expr_id)) {
        const std::size_t probe_num_distinct_values =
            estimateNumDistinctValues(expr_id, equi_join->probe());
        const double build_selectivity = estimateSelectivity(equi_join->build());
        const std::size_t estimated_num_distinct_values =
            probe_num_distinct_values * build_selectivity * filter_selectivity;
        return std::max(estimated_num_distinct_values, kOne);
      }
      if (ContainsExprId(equi_join->build()->getOutputAttributes(), expr_id)) {
        const std::size_t build_num_distinct_values =
            estimateNumDistinctValues(expr_id, equi_join->build());
        const double probe_selectivity = estimateSelectivity(equi_join->probe());
        const std::size_t estimated_num_distinct_values =
            build_num_distinct_values * probe_selectivity * filter_selectivity;
        return std::max(estimated_num_distinct_values, kOne);
      }
    }
    default:
      break;
  }
  return std::max(estimateCardinality(plan) / 10, kOne);
}

std::size_t SimpleCostModel::estimateNumDistinctValuesForTableReference(
    const ExprId expr_id,
    const TableReferencePtr &table_reference) const {
  const attribute_id attr_id =
      findCatalogRelationAttributeId(table_reference, expr_id);
  if (attr_id != kInvalidAttributeID) {
    const AttributeStatistics &stat =
        table_reference->relation().getStatistics().getAttributeStatistics(attr_id);
    if (stat.hasNumDistinctValues()) {
      return stat.getNumDistinctValues();
    }
  }
  return estimateCardinalityForTableReference(table_reference) * 0.1;
}

double SimpleCostModel::estimateDuplicationFactor(const ExprId expr_id,
                                                  const PlanPtr &plan) const {
  TableReferencePtr table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(plan, &table_reference)) {
    EquiJoinPtr equi_join;
    if (SomeEquiJoin::MatchesWithConditionalCast(plan, &equi_join)) {
      const std::size_t num_join_attributes =
          equi_join->probe_attributes().size();
      for (std::size_t i = 0; i < num_join_attributes; ++i) {
        const auto &probe_attribute = equi_join->probe_attributes().at(i);
        const auto &build_attribute = equi_join->build_attributes().at(i);
        if (probe_attribute->id() == expr_id || build_attribute->id() == expr_id) {
          const std::size_t probe_factor =
              estimateDuplicationFactor(probe_attribute->id(), equi_join->probe());
          const std::size_t build_factor =
              estimateDuplicationFactor(build_attribute->id(), equi_join->build());
          return std::max(probe_factor, build_factor);
        }
      }
    }
    for (const auto &child : plan->children()) {
      if (ContainsExprId(child->getOutputAttributes(), expr_id)) {
        return estimateDuplicationFactor(expr_id, child);
      }
    }
    return 5.0;
  }

  const attribute_id attr_id =
      findCatalogRelationAttributeId(table_reference, expr_id);
  if (attr_id == kInvalidAttributeID) {
    return 5.0;
  }


  const AttributeStatistics &stat =
      table_reference->relation().getStatistics().getAttributeStatistics(attr_id);
  if (!stat.hasNumDistinctValues()) {
    return 5.0;
  }

  const double num_tuples =
      table_reference->relation().getStatistics().getNumTuples();
  return num_tuples / std::max(stat.getNumDistinctValues(), kOne);
}

bool SimpleCostModel::impliesUniqueAttributes(
    const PlanPtr &plan,
    const std::vector<AttributeReferencePtr> &attributes) const {
  return impliesUniqueAttributes(plan, ToExprIdVector(attributes));
}

bool SimpleCostModel::impliesUniqueAttributes(
    const PlanPtr &plan,
    const std::vector<ExprId> &attributes) const {
  switch (plan->getPlanType()) {
    case PlanType::kTableReference: {
      const TableReferencePtr &table_reference =
          std::static_pointer_cast<const TableReference>(plan);
      const RelationStatistics &relation_stat =
          table_reference->relation().getStatistics();
      const std::size_t num_tuples = relation_stat.getNumTuples();
      for (const ExprId expr_id : attributes) {
        const attribute_id attr_id =
            findCatalogRelationAttributeId(table_reference, expr_id);
        if (attr_id != kInvalidAttributeID) {
          const AttributeStatistics &attr_stat =
              relation_stat.getAttributeStatistics(attr_id);
          if (attr_stat.hasNumDistinctValues() &&
              attr_stat.getNumDistinctValues() == num_tuples) {
            return true;
          }
        }
      }
      return false;
    }
    case PlanType::kSelection:  // Fall through
    case PlanType::kTableView: {
      DCHECK_EQ(plan->getNumChildren(), 1u);
      return impliesUniqueAttributes(plan->children().front(), attributes);
    }
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(plan);
      bool unique_from_left =
          impliesUniqueAttributes(equi_join->probe(), equi_join->probe_attributes())
              && impliesUniqueAttributes(equi_join->build(), attributes);
      bool unique_from_right =
          impliesUniqueAttributes(equi_join->build(), equi_join->build_attributes())
              && impliesUniqueAttributes(equi_join->probe(), attributes);
      return unique_from_left || unique_from_right;
    }
    default:
      break;
  }
  return false;
}

const Attribute* SimpleCostModel::findSourceAttribute(
    const ExprId expr_id, const PlanPtr &plan) const {
  TableReferencePtr table_reference;
  if (SomeTableReference::MatchesWithConditionalCast(plan, &table_reference)) {
    const attribute_id id = findCatalogRelationAttributeId(table_reference, expr_id);
    if (id != kInvalidAttributeID) {
      return &table_reference->relation().getAttribute(id);
    }
    return nullptr;
  }

  for (const auto &child : plan->children()) {
    if (ContainsExprId(child->getOutputAttributes(), expr_id)) {
      return findSourceAttribute(expr_id, child);
    }
  }
  return nullptr;
}

bool SimpleCostModel::isPrimaryKeyForeignKey(
    const Database &database,
    const ExprId pk_id, const PlanPtr &pk_plan,
    const ExprId fk_id, const PlanPtr &fk_plan) const {
  const Attribute *pk_attr = findSourceAttribute(pk_id, pk_plan);
  if (pk_attr == nullptr) {
    return false;
  }

  const Attribute *fk_attr = findSourceAttribute(fk_id, fk_plan);
  if (fk_attr == nullptr) {
    return false;
  }

  return database.isPrimaryKeyForeignKey(pk_attr, fk_attr);
}

std::uint32_t SimpleCostModel::inferMaxCount(
    const ExprId expr_id, const PlanPtr &plan) const {
  switch (plan->getPlanType()) {
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(plan);
      return inferMaxCount(expr_id, selection->input());
    }
    case PlanType::kTableView: {
      const TableViewPtr &table_view =
          std::static_pointer_cast<const TableView>(plan);
      return inferMaxCount(expr_id, table_view->input());
    }
    case PlanType::kTableReference: {
      const Attribute *source = findSourceAttribute(expr_id, plan);
      if (source == nullptr) {
        break;
      }

      const TableReferencePtr &table_reference =
          std::static_pointer_cast<const TableReference>(plan);

      const IndexManager &index_manager =
          table_reference->relation().getIndexManager();
      const attribute_id id = source->id();

      if (index_manager.hasPrimaryKeyIndex(id)) {
        return 1u;
      }
      if (index_manager.hasForeignKeyIndex(id)) {
        return index_manager.getForeignKeyIndex(id).getMaxCount();
      }
      break;
    }
    default:
      break;
  }
  return std::numeric_limits<std::uint32_t>::max();
}

Range SimpleCostModel::inferRange(const ExprId expr_id,
                                  const PlanPtr &plan) const {
  return inferRangeInternal(expr_id, plan, Range::Max());
}

Range SimpleCostModel::inferRangeInternal(const ExprId expr_id,
                                          const PlanPtr &plan,
                                          const Range &input) const {
  Range range = input;

  switch (plan->getPlanType()) {
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &join =
          std::static_pointer_cast<const EquiJoin>(plan);

      range = inferRangeForEquiJoin(expr_id,
                                    { join->probe(), join->build() },
                                    { &join->probe_attributes(),
                                      &join->build_attributes() },
                                    { join->probe_filter_predicate(),
                                      join->build_filter_predicate() },
                                    range);
      break;
    }
    case PlanType::kEquiJoinAggregate: {
      const EquiJoinAggregatePtr &join =
          std::static_pointer_cast<const EquiJoinAggregate>(plan);
      range = inferRangeForEquiJoin(expr_id,
                                    { join->probe(), join->build() },
                                    { &join->probe_attributes(),
                                      &join->build_attributes() },
                                    { join->probe_filter_predicate(),
                                      join->build_filter_predicate() },
                                    range);
      break;
    }
    case PlanType::kMultiwayEquiJoinAggregate: {
      const MultiwayEquiJoinAggregatePtr &join =
          std::static_pointer_cast<const MultiwayEquiJoinAggregate>(plan);

      std::vector<std::vector<AttributeReferencePtr>> attributes;
      std::vector<const std::vector<AttributeReferencePtr>*> attribute_references;
      for (const auto &attr : join->join_attributes()) {
        attributes.emplace_back();
        attributes.back().emplace_back(attr);
      }
      for (const auto &attrs : attributes) {
        attribute_references.emplace_back(&attrs);
      }

      range = inferRangeForEquiJoin(expr_id,
                                    join->inputs(),
                                    attribute_references,
                                    join->filter_predicates(),
                                    range);
      break;
    }
    default: {
      for (const auto &child : plan->children()) {
        if (ContainsExprId(child->getOutputAttributes(), expr_id)) {
          range = inferRangeInternal(expr_id, child, range);
        }
      }
    }
  }

  std::vector<PredicatePtr> filter_predicates;
  switch (plan->getPlanType()) {
    case PlanType::kSelection: {
      filter_predicates.emplace_back(
          std::static_pointer_cast<const Selection>(plan)->filter_predicate());
      break;
    }
    case PlanType::kTableView: {
      filter_predicates.emplace_back(
          std::static_pointer_cast<const TableView>(plan)->comparison());
      break;
    }
    default:
      break;
  }

  for (const auto &predicate : filter_predicates) {
    if (predicate != nullptr) {
      range = predicate->reduceRange(expr_id, range);
    }
  }

  TableReferencePtr table_reference;
  if (!SomeTableReference::MatchesWithConditionalCast(plan, &table_reference)) {
    return range;
  }

  const attribute_id attribute =
      findCatalogRelationAttributeId(table_reference, expr_id);

  if (attribute == kInvalidAttributeID) {
    return range;
  }

  const AttributeStatistics &stat =
      table_reference->relation().getStatistics().getAttributeStatistics(attribute);

  std::uint64_t min_value = range.begin();
  std::uint64_t max_value = range.end() - 1;

  if (stat.hasMinValue()) {
    min_value = std::max(min_value, stat.getMinValue());
  }
  if (stat.hasMaxValue()) {
    max_value = std::min(max_value, stat.getMaxValue());
  }

  // TODO(robin-team): Reduce plan to print NULL values.
  if (min_value > max_value) {
    return Range();
  }
  return Range(min_value, max_value + 1).intersect(range);
}

Range SimpleCostModel::inferRangeForEquiJoin(
    const ExprId expr_id,
    const std::vector<PlanPtr> &plans,
    const std::vector<const std::vector<AttributeReferencePtr>*> &join_attributes,
    const std::vector<PredicatePtr> &filter_predicates,
    const Range &input) const {
  const std::size_t num_components = plans.size();
  DCHECK_GT(num_components, 0);
  DCHECK_EQ(num_components, join_attributes.size());
  DCHECK_EQ(num_components, filter_predicates.size());

  const std::size_t num_join_attributes = join_attributes.front()->size();

  int plan_index = -1;
  int join_attribute_index = -1;

  for (std::size_t i = 0; i < num_components; ++i) {
    if (ContainsExprId(plans[i]->getOutputAttributes(), expr_id)) {
      for (std::size_t j = 0; j < num_join_attributes; ++j) {
        if (join_attributes[i]->at(j)->id() == expr_id) {
          join_attribute_index = j;
          break;
        }
      }
      plan_index = i;
      break;
    }
  }

  if (plan_index < 0) {
    return input;
  }

  Range range = input;

  if (join_attribute_index >= 0) {
    for (std::size_t i = 0; i < num_components; ++i) {
      const ExprId attr_id = join_attributes[i]->at(join_attribute_index)->id();
      range = inferRangeInternal(attr_id, plans[i], range);
      if (filter_predicates[i] != nullptr) {
        range = filter_predicates[i]->reduceRange(attr_id, range);
      }
    }
  } else {
    range = inferRangeInternal(expr_id, plans[plan_index], range);
    if (filter_predicates[plan_index] != nullptr) {
      range = filter_predicates[plan_index]->reduceRange(expr_id, range);
    }
  }

  return range;
}


std::vector<Range> SimpleCostModel::inferJoinAttributeRanges(
    const ChainedJoinAggregatePtr &chained_join_aggregate) const {
  const auto &components = chained_join_aggregate->inputs();
  DCHECK_GT(components.size(), 2u);

  std::vector<Range> ranges;
  for (std::size_t i = 0; i < components.size()-1; ++i) {
    const auto &pair = chained_join_aggregate->join_attribute_pairs()[i];

    const ExprId build_attr_id = pair.first->id();
    const ExprId probe_attr_id = pair.second->id();

    Range build_range = inferRange(build_attr_id, components[i]);
    Range probe_range = inferRange(probe_attr_id, components[i+1]);

    const auto &build_filter_predicate =
        chained_join_aggregate->filter_predicates()[i];
    const auto &probe_filter_predicate =
        chained_join_aggregate->filter_predicates()[i+1];

    if (build_filter_predicate != nullptr) {
      build_range = build_filter_predicate->reduceRange(build_attr_id, build_range);
    }
    if (probe_filter_predicate != nullptr) {
      probe_range = probe_filter_predicate->reduceRange(probe_attr_id, probe_range);
    }
    ranges.emplace_back(build_range.intersect(probe_range));
  }

  return ranges;
}

attribute_id SimpleCostModel::findCatalogRelationAttributeId(
    const TableReferencePtr &table_reference,
    const ExprId expr_id) const {
  const auto &attribute_list = table_reference->attribute_list();
  for (std::size_t i = 0; i < attribute_list.size(); ++i) {
    if (attribute_list[i]->id() == expr_id) {
      return i;
    }
  }
  return kInvalidAttributeID;
}

}  // namespace optimizer
}  // namespace project
