#include "optimizer/rules/EliminatePredicateLiteral.hpp"

#include <vector>

#include "optimizer/Aggregate.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/EquiJoinAggregate.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/PredicateLiteral.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/TableReference.hpp"
#include "optimizer/TableView.hpp"
#include "optimizer/TopLevelPlan.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr EliminatePredicateLiteral::apply(const PlanPtr &input) {
  DCHECK(input->getPlanType() == PlanType::kTopLevelPlan);

  const TopLevelPlanPtr &top_level_plan =
      std::static_pointer_cast<const TopLevelPlan>(input);

  PlanPtr output = applyInternal(top_level_plan->plan());

  if (output == top_level_plan->plan()) {
    return input;
  }

  if (output == nullptr) {
    const auto output_attributes = input->getOutputAttributes();
    std::vector<const Type*> types;
    types.reserve(output_attributes.size());
    for (const auto &attr : output_attributes) {
      types.emplace_back(&attr->getValueType());
    }

    const relation_id rel_id =
        database_->addRelation(new Relation(types, true /* is_temporary */));

    output = TableReference::Create(database_->getRelation(rel_id),
                                    optimizer_context_);
  }

  return input->copyWithNewChildren({output});
}

PlanPtr EliminatePredicateLiteral::applyInternal(const PlanPtr &node) {
  // TODO(robin-team): May support elimination of "true".
  for (const auto &child : node->children()) {
    const PlanPtr transformed = applyInternal(child);
    if (transformed == nullptr) {
      return nullptr;
    }
  }

  std::vector<PredicatePtr> filter_predicates;
  switch (node->getPlanType()) {
    case PlanType::kAggregate:
      filter_predicates.emplace_back(
          std::static_pointer_cast<const Aggregate>(node)->filter_predicate());
      break;
    case PlanType::kSelection:
      filter_predicates.emplace_back(
          std::static_pointer_cast<const Selection>(node)->filter_predicate());
      break;
    case PlanType::kTableView: {
      filter_predicates.emplace_back(
          std::static_pointer_cast<const TableView>(node)->comparison());
      break;
    }
    case PlanType::kEquiJoin: {
      const EquiJoinPtr &equi_join =
          std::static_pointer_cast<const EquiJoin>(node);
      filter_predicates.emplace_back(equi_join->probe_filter_predicate());
      filter_predicates.emplace_back(equi_join->build_filter_predicate());
      break;
    }
    case PlanType::kEquiJoinAggregate: {
      const EquiJoinAggregatePtr &equi_join_agg =
          std::static_pointer_cast<const EquiJoinAggregate>(node);
      filter_predicates.emplace_back(equi_join_agg->probe_filter_predicate());
      filter_predicates.emplace_back(equi_join_agg->build_filter_predicate());
      break;
    }
    default:
      break;
  }

  for (const auto &filter_predicate : filter_predicates) {
    if (filter_predicate == nullptr) {
      continue;
    }

    const auto components = GetConjunctivePredicates(filter_predicate);
    for (const auto &component : components) {
      PredicateLiteralPtr literal;
      if (SomePredicateLiteral::MatchesWithConditionalCast(component, &literal) &&
          literal->value() == false) {
        return nullptr;
      }
    }
  }
  return node;
}

}  // namespace optimizer
}  // namespace project
