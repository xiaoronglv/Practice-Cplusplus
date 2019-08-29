#include "optimizer/rules/ReorderJoins.hpp"

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "optimizer/Alias.hpp"
#include "optimizer/AttributeReference.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "storage/Attribute.hpp"
#include "storage/Database.hpp"
#include "storage/Relation.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/DisjointSetForest.hpp"
#include "utility/HashPair.hpp"
#include "utility/MemoryUtil.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

namespace {

using PrimaryKeyForeignKeyIndex = std::unordered_set<std::pair<ExprId, ExprId>>;

struct TableInfo {
  std::size_t idx;
  PlanPtr table;
  std::size_t estimated_cardinality;
  double estimated_selectivity;
  std::size_t num_output_attributes;
};

struct JoinPair {
  inline static bool IsPrimaryKeyForeignKey(
      const JoinPair &side, const PrimaryKeyForeignKeyIndex &pk_fk_index) {
    if (side.build_attributes.size() != 1) {
      return false;
    }
    DCHECK_EQ(side.build_attributes.size(), side.probe_attributes.size());
    const auto it =
        pk_fk_index.find(std::make_pair(side.build_attributes.front(),
                                        side.probe_attributes.front()));
    return it != pk_fk_index.end();
  }

  inline bool isBetterThan(const JoinPair &rhs,
                           const PrimaryKeyForeignKeyIndex &pk_fk_index) const {
    const auto &lhs = *this;
    bool lhs_status, rhs_status;
    std::size_t lhs_value, rhs_value;

#define BOOL_RULE(p) \
    lhs_status = p(lhs); \
    rhs_status = p(rhs); \
    if (lhs_status != rhs_status) return lhs_status;

#define LESS_RULE(p) \
    lhs_value = p(lhs); \
    rhs_value = p(rhs); \
    if (lhs_value != rhs_value) return lhs_value < rhs_value;

    // Prefer foreign-key primary-key style hash joins.
    BOOL_RULE([&](const auto &side) {
      return IsPrimaryKeyForeignKey(side, pk_fk_index);
    });

    if (lhs_status) {
      DCHECK(rhs_status);

      BOOL_RULE([&](const auto &side) {
        return side.build->num_output_attributes == 1 &&
               side.build->table->getPlanType() == PlanType::kTableReference;
      });
    }

    BOOL_RULE([](const auto &side) {
      return side.build_side_primary_key;
    });

    // Prefer hash joins where the build side table has no output attributes.
    BOOL_RULE([](const auto &side) {
      return side.build->num_output_attributes <= side.num_join_attributes;
    });

    // Prefer hash joins where build side is small.
    BOOL_RULE([](const auto &side) {
      return side.build->estimated_cardinality < 100;
    });

    // Prefer hash joins where build side has low selectivity.
    BOOL_RULE([](const auto &side) {
      return side.build->estimated_selectivity < 0.1;
    });

    // Prefer hash joins where probe side is small.
    BOOL_RULE([](const auto &side) {
      return side.probe->estimated_cardinality < 100;
    });

    // Prefer hash joins where probe side has low selectivity.
    BOOL_RULE([](const auto &side) {
      return side.probe->estimated_selectivity < 0.1;
    });

    // Prefer hash joins where the expected output cardinality is smaller.
    LESS_RULE([](const auto &side) {
      return std::max(side.probe->estimated_cardinality * side.build->estimated_selectivity,
                      side.build->estimated_cardinality * side.probe->estimated_selectivity);
    });

#undef BOOL_RULE
#undef LESS_RULE

    return false;
  }

  TableInfo *probe;
  TableInfo *build;
  std::vector<ExprId> probe_attributes;
  std::vector<ExprId> build_attributes;
  bool build_side_primary_key;
  std::size_t num_join_attributes;
};

}  // namespace

PlanPtr ReorderJoins::apply(const PlanPtr &input) {
//  return input;
  return applyInternal(input, nullptr);
}

PlanPtr ReorderJoins::applyInternal(const PlanPtr &input,
                                    JoinGroupInfo *parent_join_group) const {
  EquiJoinPtr equi_join;
  const bool is_hash_inner_join =
      SomeEquiJoin::MatchesWithConditionalCast(input, &equi_join)
          && equi_join->join_type() == EquiJoin::kHashInnerJoin;

  if (is_hash_inner_join) {
    std::unique_ptr<JoinGroupInfo> new_join_group;
    JoinGroupInfo *join_group = nullptr;
    if (parent_join_group == nullptr) {
      new_join_group.reset(new JoinGroupInfo());
      join_group = new_join_group.get();
    } else {
      join_group = parent_join_group;
    }

    // Gather tables into the join group.
    for (const PlanPtr &child : input->children()) {
      applyInternal(child, join_group);
    }

    // Gather join attribute pairs.
    const auto &probe_attributes = equi_join->probe_attributes();
    const auto &build_attributes = equi_join->build_attributes();
    DCHECK_EQ(probe_attributes.size(), build_attributes.size());
    for (std::size_t i = 0; i < probe_attributes.size(); ++i) {
      join_group->join_attribute_pairs.emplace_back(probe_attributes[i]->id(),
                                                    build_attributes[i]->id());
    }

    if (join_group != parent_join_group) {
      // This node is the root node for a group of hash inner joins. Now plan the
      // ordering of the joins.
      const PlanPtr output = generatePlan(*join_group,
                                          equi_join->project_expressions());
      if (parent_join_group == nullptr) {
        return output;
      } else {
        parent_join_group->tables.emplace_back(output);
        return nullptr;
      }
    } else {
      return nullptr;
    }
  } else {
    std::vector<PlanPtr> new_children;
    new_children.reserve(input->getNumChildren());
    for (const PlanPtr &child : input->children()) {
      new_children.emplace_back(applyInternal(child, nullptr));
    }

    PlanPtr output;
    if (new_children == input->children()) {
      output = input;
    } else {
      output = input->copyWithNewChildren(new_children);
    }

    if (parent_join_group == nullptr) {
      return output;
    } else {
      parent_join_group->tables.emplace_back(output);
      return nullptr;
    }
  }
}

PlanPtr ReorderJoins::generatePlan(
    const JoinGroupInfo &join_group,
    const std::vector<ScalarPtr> &project_expressions) const {
  std::unordered_set<ExprId> project_attributes;
  for (const ScalarPtr &scalar : project_expressions) {
    for (const AttributeReferencePtr &attr : scalar->getReferencedAttributes()) {
      project_attributes.emplace(attr->id());
    }
  }

  std::unordered_set<ExprId> referenced_attributes = project_attributes;
  for (const auto &it : join_group.join_attribute_pairs) {
    referenced_attributes.emplace(it.first);
    referenced_attributes.emplace(it.second);
  }

  const std::vector<PlanPtr> &tables = join_group.tables;
  const std::size_t num_tables = tables.size();
  std::vector<TableInfo> table_info_storage;

  for (std::size_t i = 0; i < num_tables; ++i) {
    const PlanPtr &table = tables[i];
    std::size_t num_output_attributes = 0;
    for (const auto &attr : table->getOutputAttributes()) {
      if (referenced_attributes.find(attr->id()) != referenced_attributes.end()) {
        ++num_output_attributes;
      }
    }

    table_info_storage.emplace_back(TableInfo{
        i,
        table,
        cost_model_.estimateCardinality(table),
        cost_model_.estimateSelectivity(table),
        num_output_attributes
    });
  }

  std::vector<std::pair<ExprId, const Attribute*>> pk_info_;
  std::vector<std::pair<ExprId, const Attribute*>> fk_info_;

  std::unordered_map<ExprId, std::size_t> attribute_to_table_map;
  std::unordered_map<ExprId, AttributeReferencePtr> attribute_to_reference_map;

  for (std::size_t i = 0; i < num_tables; ++i) {
    for (const auto &attr : tables[i]->getOutputAttributes()) {
      const ExprId id = attr->id();
      attribute_to_table_map.emplace(id, i);
      attribute_to_reference_map.emplace(id, attr);

      const Attribute *sa = cost_model_.findSourceAttribute(id, tables[i]);
      const Relation &relation = sa->getParentRelation();
      if (relation.isPrimaryKey(sa->id())) {
        pk_info_.emplace_back(id, sa);
      }
      if (database_.isForeignKey(sa)) {
        fk_info_.emplace_back(id, sa);
      }
    }
  }

  PrimaryKeyForeignKeyIndex pk_fk_index;
  for (const auto &pk_pair : pk_info_) {
    for (const auto &fk_pair : fk_info_) {
      if (database_.isPrimaryKeyForeignKey(pk_pair.second, fk_pair.second)) {
        pk_fk_index.emplace(pk_pair.first, fk_pair.first);
      }
    }
  }

  // The equal-join (e.g. =) operator defines an equivalence relation on the
  // set of all the attributes. The disjoint set data structure is used to keep
  // track of the equivalence classes that each attribute belongs to.
  DisjointSetForest<ExprId> join_attribute_forest;
  for (const auto &it : join_group.join_attribute_pairs) {
    join_attribute_forest.makeSet(it.first);
    join_attribute_forest.makeSet(it.second);
    join_attribute_forest.merge(it.first, it.second);
  }

//  for (std::size_t i = 0; i < tables.size(); ++i) {
//    std::cerr << "[" << i << "]\n"
//              << tables[i]->toString()
//              << "# = " << table_info_storage[i].estimated_cardinality << "\n"
//              << "% = " << table_info_storage[i].estimated_selectivity << "\n"
//              << "output = " << table_info_storage[i].num_output_attributes
//              << "\n\n";
//  }

  // Map each equivalence class id to the members (e.g. <table id, attribute id>
  // pairs) in that equivalence class.
  using EquivalenceClass = std::unordered_map<std::size_t, ExprId>;
  std::unordered_map<std::size_t, EquivalenceClass> join_attribute_groups;
  for (const auto &it : join_group.join_attribute_pairs) {
    const std::size_t lhs = attribute_to_table_map[it.first];
    const std::size_t rhs = attribute_to_table_map[it.second];
    DCHECK_NE(lhs, rhs);

    const std::size_t group_id = join_attribute_forest.find(it.first);
    DCHECK_EQ(group_id, join_attribute_forest.find(it.second));

    auto &join_attribute_group = join_attribute_groups[group_id];
    join_attribute_group.emplace(lhs, it.first);
    join_attribute_group.emplace(rhs, it.second);
  }

  std::unordered_set<TableInfo*> remaining_tables;
  for (TableInfo &table_info : table_info_storage) {
    remaining_tables.emplace(&table_info);
  }

  EquiJoinPtr output = nullptr;

  while (true) {
    std::unique_ptr<JoinPair> best_join = nullptr;

    // Find the best probe/build pair out of the remaining tables.
    for (TableInfo *probe : remaining_tables) {
      for (TableInfo *build : remaining_tables) {
        if (probe == build) {
          continue;
        }

        const std::size_t probe_idx = probe->idx;
        const std::size_t build_idx = build->idx;

        std::vector<ExprId> probe_attributes;
        std::vector<ExprId> build_attributes;

        for (const auto &it : join_attribute_groups) {
          const auto &eqclass = it.second;
          const auto probe_attribute_it = eqclass.find(probe_idx);
          const auto build_attribute_it = eqclass.find(build_idx);
          if (probe_attribute_it != eqclass.end() &&
              build_attribute_it != eqclass.end()) {
            probe_attributes.emplace_back(probe_attribute_it->second);
            build_attributes.emplace_back(build_attribute_it->second);
          }
        }

        if (build_attributes.empty()) {
          continue;
        }

        const bool build_side_unique =
            cost_model_.impliesUniqueAttributes(build->table, build_attributes);
        std::unique_ptr<JoinPair> new_join(
            new JoinPair{ probe, build,
                          std::move(probe_attributes),
                          std::move(build_attributes),
                          build_side_unique,
                          build_attributes.size() });

        if (best_join == nullptr ||
            new_join->isBetterThan(*best_join, pk_fk_index)) {
          best_join = std::move(new_join);
        }
      }
    }

    CHECK(best_join != nullptr);

//    std::cerr << "**** PROBE ****\n"
//              << best_join->probe->table->toString() << "\n"
//              << "**** BUILD ****\n"
//              << best_join->build->table->toString() << "\n"
//              << "**** STATS ****\n"
//              << "Primary key = " << best_join->build_side_primary_key << "\n\n";

    TableInfo *selected_probe = best_join->probe;
    TableInfo *selected_build = best_join->build;

    remaining_tables.erase(selected_probe);
    remaining_tables.erase(selected_build);

    const PlanPtr &probe_child = selected_probe->table;
    const PlanPtr &build_child = selected_build->table;

    // Figure out the output attributes.
    std::vector<ScalarPtr> output_expressions;
    InsertAll(probe_child->getOutputAttributes(), &output_expressions);
    InsertAll(build_child->getOutputAttributes(), &output_expressions);

    // Figure out the join attributes.
    std::vector<AttributeReferencePtr> probe_attributes;
    std::vector<AttributeReferencePtr> build_attributes;
    const std::size_t probe_idx = selected_probe->idx;
    const std::size_t build_idx = selected_build->idx;

    for (const auto &it : join_attribute_groups) {
      const auto &attr_group = it.second;
      auto probe_it = attr_group.find(probe_idx);
      auto build_it = attr_group.find(build_idx);
      if (probe_it != attr_group.end() && build_it != attr_group.end()) {
        probe_attributes.emplace_back(
            attribute_to_reference_map.at(probe_it->second));
        build_attributes.emplace_back(
            attribute_to_reference_map.at(build_it->second));
      }
    }

    // Update join attribute groups.
    for (auto &it : join_attribute_groups) {
      auto &attr_group = it.second;
      auto build_it = attr_group.find(build_idx);
      if (build_it != attr_group.end()) {
        const ExprId attr_id = build_it->second;
        attr_group.erase(build_it);
        attr_group.emplace(probe_idx, attr_id);
      }
    }

    output = EquiJoin::Create(probe_child,
                              build_child,
                              probe_attributes,
                              build_attributes,
                              output_expressions,
                              nullptr /* probe_filter_predicate */,
                              nullptr /* build_filter_predicate */,
                              EquiJoin::kHashInnerJoin);

    if (remaining_tables.empty()) {
      break;
    }

    selected_probe->table = output;
    selected_probe->estimated_cardinality = cost_model_.estimateCardinality(output);
    selected_probe->estimated_selectivity = cost_model_.estimateSelectivity(output);

    // Update output attribute count.
    std::size_t num_erased_attributes = 0;
    for (std::size_t i = 0; i < build_attributes.size(); ++i) {
      const ExprId probe_join_id = probe_attributes[i]->id();
      const ExprId build_join_id = build_attributes[i]->id();
      if (ContainsKey(project_attributes, probe_join_id) ||
          ContainsKey(project_attributes, build_join_id)) {
        const std::size_t group_id = join_attribute_forest.find(probe_join_id);
        DCHECK_EQ(group_id, join_attribute_forest.find(build_join_id));
        DCHECK(ContainsKey(join_attribute_groups, group_id));

        if (join_attribute_groups.at(group_id).size() == 1) {
          num_erased_attributes += 1;
          continue;
        }
      }
      num_erased_attributes += 2;
    }
    selected_probe->num_output_attributes +=
        selected_build->num_output_attributes - num_erased_attributes;

    remaining_tables.emplace(selected_probe);
  }

  // Figure out the minimal set of output expressionss.
  std::vector<ScalarPtr> minimal_output_expressions;
  for (const auto &attr : output->getOutputAttributes()) {
    const ExprId attr_id = attr->id();
    if (!join_attribute_forest.hasElement(attr_id)) {
      minimal_output_expressions.emplace_back(attr);
      continue;
    }

    const std::size_t group_id = join_attribute_forest.find(attr_id);
    const auto &attr_group = join_attribute_groups.at(group_id);
    DCHECK_EQ(1u, attr_group.size());

    const AttributeReferencePtr &replacement_attr =
        attribute_to_reference_map.at(attr_group.begin()->second);
    if (attr_id == replacement_attr->id()) {
      minimal_output_expressions.emplace_back(attr);
    } else {
      minimal_output_expressions.emplace_back(
          Alias::Create(replacement_attr, attr_id, replacement_attr->name()));
    }
  }

  output = EquiJoin::Create(output->probe(),
                            output->build(),
                            output->probe_attributes(),
                            output->build_attributes(),
                            minimal_output_expressions,
                            nullptr /* probe_filter_predicate */,
                            nullptr /* build_filter_predicate */,
                            output->join_type());

  return Selection::Create(output, project_expressions, nullptr);
}

}  // namespace optimizer
}  // namespace project


