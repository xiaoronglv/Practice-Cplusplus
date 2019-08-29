#include "optimizer/rules/LiftUpExtraEquiJoinPredicate.hpp"

#include <cstddef>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Selection.hpp"
#include "storage/Attribute.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

PlanPtr LiftUpExtraEquiJoinPredicate::applyToNode(const PlanPtr &node) {
  EquiJoinPtr equi_join;
  if (!SomeEquiJoin::MatchesWithConditionalCast(node, &equi_join) ||
      equi_join->probe_attributes().size() == 1) {
    return node;
  }

  const auto &probe_attributes = equi_join->probe_attributes();
  const auto &build_attributes = equi_join->build_attributes();

  const std::size_t num_join_attributes = probe_attributes.size();
  DCHECK_GE(num_join_attributes, 2u);
  DCHECK_EQ(num_join_attributes, build_attributes.size());

  int best_score = 0;
  int best_index = 0;

  for (std::size_t i = 0; i < num_join_attributes; ++i) {
    const ExprId probe_attribute = probe_attributes.at(i)->id();
    const ExprId build_attribute = build_attributes.at(i)->id();

    int score = 0;

    // Just put some rule here ...
    const Attribute *probe_source =
        cost_model_.findSourceAttribute(probe_attribute, equi_join->probe());
    if (probe_source != nullptr) {
      const IndexManager &index_manager =
          probe_source->getParentRelation().getIndexManager();
      if (index_manager.hasPrimaryKeyIndex(probe_source->id())) {
        score += 2;
      } else if (index_manager.hasForeignKeyIndex(probe_source->id())) {
        score += 1;
      }
    }

    const Attribute *build_source =
        cost_model_.findSourceAttribute(build_attribute, equi_join->build());
    if (build_source != nullptr) {
      const IndexManager &index_manager =
          build_source->getParentRelation().getIndexManager();
      if (index_manager.hasPrimaryKeyIndex(build_source->id())) {
        score += 2;
      } else if (index_manager.hasForeignKeyIndex(build_source->id())) {
        score += 1;
      }
    }

    if (score > best_score) {
      best_score = score;
      best_index = i;
    }
  }

  std::vector<PredicatePtr> residual_predicates;
  for (std::size_t i = 0; i < num_join_attributes; ++i) {
    if (i != best_index) {
      residual_predicates.emplace_back(
          Comparison::Create(ComparisonType::kEqual,
                             probe_attributes[i],
                             build_attributes[i]));
    }
  }

  DCHECK(equi_join->probe_filter_predicate() == nullptr);
  DCHECK(equi_join->build_filter_predicate() == nullptr);

  std::vector<AttributeReferencePtr> project_attributes;
  InsertAll(equi_join->probe()->getOutputAttributes(), &project_attributes);
  InsertAll(equi_join->build()->getOutputAttributes(), &project_attributes);

  const EquiJoinPtr new_join =
      EquiJoin::Create(equi_join->probe(),
                       equi_join->build(),
                       { probe_attributes[best_index] },
                       { build_attributes[best_index] },
                       CastSharedPtrVector<const Scalar>(project_attributes),
                       equi_join->probe_filter_predicate(),
                       equi_join->build_filter_predicate(),
                       equi_join->join_type());

  return Selection::Create(new_join,
                           equi_join->project_expressions(),
                           CreateConjunctivePredicate(residual_predicates));
}

}  // namespace optimizer
}  // namespace project

