#include "optimizer/rules/GenerateJoins.hpp"

#include <cstddef>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/Filter.hpp"
#include "optimizer/MultiwayCartesianJoin.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "storage/Attribute.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/DisjointSetForest.hpp"
#include "utility/MemoryUtil.hpp"

namespace project {
namespace optimizer {

namespace {

inline bool MatchesJoinPredicate(
    const PredicatePtr &predicate,
    const std::unordered_map<ExprId, std::size_t> &attribute_to_table_map,
    ExprId *left_id, ExprId *right_id) {
  ComparisonPtr comparison;
  CHECK(SomeComparison::MatchesWithConditionalCast(predicate, &comparison));
  if (comparison->comparison_type() != ComparisonType::kEqual) {
    return false;
  }

  AttributeReferencePtr left, right;
  if (!SomeAttributeReference::MatchesWithConditionalCast(comparison->left(), &left) ||
      !SomeAttributeReference::MatchesWithConditionalCast(comparison->right(), &right)) {
    return false;
  }

  const auto left_it = attribute_to_table_map.find(left->id());
  DCHECK(left_it != attribute_to_table_map.end());

  const auto right_it = attribute_to_table_map.find(right->id());
  DCHECK(right_it != attribute_to_table_map.end());

  if (left_it->second == right_it->second) {
    return false;
  }

  *left_id = left->id();
  *right_id = right->id();
  return true;
}

struct TableInfo {
  PlanPtr table;
  std::size_t idx;
};

}  // namespace

PlanPtr GenerateJoins::applyToNode(const PlanPtr &node) {
  FilterPtr filter;
  MultiwayCartesianJoinPtr cartesian_join;

  if (!SomeFilter::MatchesWithConditionalCast(node, &filter) ||
      !SomeMultiwayCartesianJoin::MatchesWithConditionalCast(filter->input(), &cartesian_join)) {
    return node;
  }

  const std::vector<PlanPtr> &tables = cartesian_join->operands();
  const std::size_t num_tables = tables.size();

  std::vector<TableInfo> table_info_storage(num_tables);
  std::unordered_map<ExprId, std::size_t> attribute_to_table_map;
  std::unordered_map<ExprId, AttributeReferencePtr> attribute_to_reference_map;
  for (std::size_t i = 0; i < num_tables; ++i) {
    for (const auto &attr : tables[i]->getOutputAttributes()) {
      attribute_to_table_map.emplace(attr->id(), i);
      attribute_to_reference_map.emplace(attr->id(), attr);
    }
    table_info_storage[i] = { tables[i], i };
  }

  const std::vector<PredicatePtr> predicates =
      GetConjunctivePredicates(filter->filter_predicate());

  std::vector<PredicatePtr> non_join_predicates;
  std::vector<std::pair<ExprId, ExprId>> join_attribute_pairs;

  // The equal-join (e.g. =) operator defines an equivalence relation on the
  // set of all the attributes. The disjoint set data structure is used to keep
  // track of the equivalence classes that each attribute belongs to.
  DisjointSetForest<ExprId> join_attribute_forest;

  for (const PredicatePtr &predicate : predicates) {
    ExprId left_id, right_id;
    if (MatchesJoinPredicate(predicate, attribute_to_table_map,
                             &left_id, &right_id)) {
      join_attribute_pairs.emplace_back(left_id, right_id);
      join_attribute_forest.makeSet(left_id);
      join_attribute_forest.makeSet(right_id);
      join_attribute_forest.merge(left_id, right_id);
    } else {
      non_join_predicates.emplace_back(predicate);
    }
  }

  // Map each equivalence class id to the members (e.g. <table id, attribute id>
  // pairs) in that equivalence class.
  using EquivalenceClass = std::unordered_map<std::size_t, std::set<ExprId>>;
  std::unordered_map<std::size_t, EquivalenceClass> join_attribute_groups;
  for (const auto &attr_pair : join_attribute_pairs) {
    const std::size_t first_table_idx =
        attribute_to_table_map[attr_pair.first];
    const std::size_t second_table_idx =
        attribute_to_table_map[attr_pair.second];
    const std::size_t attr_group_id =
        join_attribute_forest.find(attr_pair.first);
    auto &attr_group = join_attribute_groups[attr_group_id];
    attr_group[first_table_idx].emplace(attr_pair.first);
    attr_group[second_table_idx].emplace(attr_pair.second);
  }

  // First try to break cycles as filter predicates on base tables.
  for (auto &group_it : join_attribute_groups) {
    for (auto &table_it : group_it.second) {
      auto &attr_set = table_it.second;
      const std::size_t num_attrs = attr_set.size();
      if (num_attrs > 1) {
        std::vector<ExprId> attrs(attr_set.begin(), attr_set.end());
        for (std::size_t i = 1; i < num_attrs; ++i) {
          non_join_predicates.emplace_back(
              Comparison::Create(ComparisonType::kEqual,
                                 attribute_to_reference_map.at(attrs[0]),
                                 attribute_to_reference_map.at(attrs[i])));
        }
      }
    }
  }

  std::unordered_set<TableInfo*> remaining_tables;
  for (auto &table_info : table_info_storage) {
    remaining_tables.emplace(&table_info);
  }

  while (true) {
    // Join order optimization will be done later.
    TableInfo *selected_probe_table_info = nullptr;
    TableInfo *selected_build_table_info = nullptr;

    for (const auto &group_it : join_attribute_groups) {
      if (group_it.second.size() < 2) {
        continue;
      }
      auto it = group_it.second.begin();
      selected_probe_table_info = &table_info_storage[it->first];
      ++it;
      selected_build_table_info = &table_info_storage[it->first];
    }

    if (selected_probe_table_info == nullptr) {
      LOG(FATAL) << "Unexpected cartesian product at GenerateJoins.";
    }
    DCHECK(selected_build_table_info != nullptr);

    remaining_tables.erase(selected_probe_table_info);
    remaining_tables.erase(selected_build_table_info);

    // Figure out the output attributes.
    const PlanPtr &probe_child = selected_probe_table_info->table;
    const PlanPtr &build_child = selected_build_table_info->table;
    auto output_attributes = probe_child->getOutputAttributes();
    InsertAll(build_child->getOutputAttributes(), &output_attributes);

    // Figure out the join attributes.
    std::vector<AttributeReferencePtr> probe_attributes;
    std::vector<AttributeReferencePtr> build_attributes;
    const std::size_t probe_table_idx = selected_probe_table_info->idx;
    const std::size_t build_table_idx = selected_build_table_info->idx;

    for (const auto &group_it : join_attribute_groups) {
      const auto &attr_group = group_it.second;
      auto probe_it = attr_group.find(probe_table_idx);
      auto build_it = attr_group.find(build_table_idx);
      if (probe_it != attr_group.end() && build_it != attr_group.end()) {
        DCHECK(!probe_it->second.empty());
        DCHECK(!build_it->second.empty());

        // Prefer fk-pk pairs.
        const std::vector<ExprId> probe_attrs(probe_it->second.begin(),
                                              probe_it->second.end());
        const std::vector<ExprId> build_attrs(build_it->second.begin(),
                                              build_it->second.end());
        ExprId best_probe_attr = probe_attrs.front();
        ExprId best_build_attr = build_attrs.front();
        bool found = false;
        for (const ExprId probe_id : probe_attrs) {
          const Attribute *probe_attr =
              cost_model_.findSourceAttribute(
                  probe_id, tables[attribute_to_table_map.at(probe_id)]);
          for (const ExprId build_id : build_attrs) {
            const Attribute *build_attr =
                cost_model_.findSourceAttribute(
                    build_id, tables[attribute_to_table_map.at(build_id)]);
            if (database_.isPrimaryKeyForeignKey(probe_attr, build_attr) ||
                database_.isPrimaryKeyForeignKey(build_attr, probe_attr)) {
              best_probe_attr = probe_id;
              best_build_attr = build_id;
              found = true;
              break;
            }
          }
          if (found) {
            break;
          }
        }

        probe_attributes.emplace_back(
            attribute_to_reference_map.at(best_probe_attr));
        build_attributes.emplace_back(
            attribute_to_reference_map.at(best_build_attr));
      }
    }

    const PlanPtr output = EquiJoin::Create(
        probe_child,
        build_child,
        probe_attributes,
        build_attributes,
        CastSharedPtrVector<const Scalar>(output_attributes),
        nullptr /* probe_filter_predicate */,
        nullptr /* build_filter_predicate */,
        EquiJoin::kHashInnerJoin);

    // Return the last table in the table pool if there is only one table left.
    if (remaining_tables.empty()) {
      if (non_join_predicates.empty()) {
        return output;
      } else {
        return Filter::Create(output,
                              CreateConjunctivePredicate(non_join_predicates));
      }
    }

    // Create a hash join from the choosen probe/build pair and put it back to
    // the table pool.
    selected_probe_table_info->table = output;
    remaining_tables.emplace(selected_probe_table_info);

    // Update join attribute groups.
    for (auto &group_it : join_attribute_groups) {
      auto &attr_group = group_it.second;
      auto build_it = attr_group.find(build_table_idx);
      if (build_it == attr_group.end()) {
        continue;
      }

      auto &probe_attrs = attr_group[probe_table_idx];
      for (const ExprId attr_id : build_it->second) {
        probe_attrs.emplace(attr_id);
      }

      attr_group.erase(build_it);
    }
  }
}

}  // namespace optimizer
}  // namespace project
