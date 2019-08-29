#include "optimizer/rules/PopulateFilterOnJoinAttributes.hpp"

#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Filter.hpp"
#include "optimizer/MultiwayCartesianJoin.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "utility/DisjointSetForest.hpp"

namespace project {
namespace optimizer {

namespace {

inline bool MatchesJoinPredicate(
    const ComparisonPtr &comparison,
    AttributeReferencePtr *right) {
  if (comparison->comparison_type() != ComparisonType::kEqual) {
    return false;
  }

  return SomeAttributeReference::MatchesWithConditionalCast(comparison->right(), right);
}

}  // namespace

PlanPtr PopulateFilterOnJoinAttributes::applyToNode(const PlanPtr &node) {
  FilterPtr filter;
  MultiwayCartesianJoinPtr cartesian_join;

  if (!SomeFilter::MatchesWithConditionalCast(node, &filter) ||
      !SomeMultiwayCartesianJoin::MatchesWithConditionalCast(filter->input(), &cartesian_join)) {
    return node;
  }

  std::vector<PredicatePtr> predicates =
      GetConjunctivePredicates(filter->filter_predicate());

  // TODO(zuyu): make it general to any query.
  ComparisonPtr filter_predicate;
  // NOTE(zuyu): it is possible that an attribute is both a filter attribute and
  // a join one.
  ExprId filter_attribute = kInvalidExprId;
  std::unordered_map<ExprId, AttributeReferencePtr> join_attributes;
  join_attributes.reserve(4);

  // The equal-join (e.g. =) operator defines an equivalence relation on the
  // set of all the attributes. The disjoint set data structure is used to keep
  // track of the equivalence classes that each attribute belongs to.
  DisjointSetForest<ExprId> join_attribute_forest;
  for (const PredicatePtr &predicate : predicates) {
    ComparisonPtr comparison;
    CHECK(SomeComparison::MatchesWithConditionalCast(predicate, &comparison));

    AttributeReferencePtr left;
    CHECK(SomeAttributeReference::MatchesWithConditionalCast(comparison->left(), &left));
    const ExprId left_id = left->id();

    AttributeReferencePtr right;
    if (MatchesJoinPredicate(comparison, &right)) {
      join_attributes.emplace(left_id, std::move(left));

      const ExprId right_id = right->id();
      join_attributes.emplace(right_id, std::move(right));

      join_attribute_forest.makeSet(left_id);
      join_attribute_forest.makeSet(right_id);
      join_attribute_forest.merge(left_id, right_id);
    } else {
      filter_predicate = std::move(comparison);
      DCHECK_EQ(kInvalidExprId, filter_attribute);
      filter_attribute = left_id;
    }
  }

  const auto cit = join_attributes.find(filter_attribute);
  if (cit == join_attributes.end()) {
    return node;
  }
  join_attributes.erase(cit);

  const std::size_t num_predicates = predicates.size();
  for (const auto &attr_info : join_attributes) {
    if (join_attribute_forest.isConnected(filter_attribute, attr_info.first)) {
      predicates.emplace_back(Comparison::Create(filter_predicate->comparison_type(),
                                                 attr_info.second,
                                                 filter_predicate->right()));
    }
  }

  if (predicates.size() == num_predicates) {
    return node;
  }

  return Filter::Create(filter->input(), CreateConjunctivePredicate(predicates));
}

}  // namespace optimizer
}  // namespace project
