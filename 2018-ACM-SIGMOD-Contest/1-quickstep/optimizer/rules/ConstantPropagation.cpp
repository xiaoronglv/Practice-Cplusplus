#include "optimizer/rules/ConstantPropagation.hpp"

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Comparison.hpp"
#include "optimizer/EquiJoin.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/ExpressionUtil.hpp"
#include "optimizer/OptimizerContext.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/PredicateLiteral.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/ScalarLiteral.hpp"
#include "optimizer/Selection.hpp"
#include "optimizer/rules/CollapseSelection.hpp"
#include "storage/Attribute.hpp"
#include "storage/IndexManager.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

namespace project {
namespace optimizer {

PlanPtr ConstantPropagation::apply(const PlanPtr &input) {
  collectConstants(input);

  if (constants_.empty()) {
    return input;
  }

  std::unordered_set<ExprId> filtered;
  const PlanPtr output = attachFilters(input, &filtered);

  CollapseSelection collapse_selection;
  return collapse_selection.apply(output);
}

void ConstantPropagation::collectConstants(const PlanPtr &node) {
  // First process child nodes
  for (const auto &child : node->children()) {
    collectConstants(child);
  }

  PredicatePtr filter_predicate = nullptr;
  switch (node->getPlanType()) {
    case PlanType::kSelection: {
      const SelectionPtr &selection =
          std::static_pointer_cast<const Selection>(node);
      filter_predicate = selection->filter_predicate();
    }
    default:
      break;
  }

  if (filter_predicate != nullptr) {
    const std::vector<PredicatePtr> predicates =
        GetConjunctivePredicates(filter_predicate);
    for (const auto &predicate : predicates) {
      ComparisonPtr comparison;
      AttributeReferencePtr attribute;
      ScalarLiteralPtr literal;
      if (SomeComparison::MatchesWithConditionalCast(predicate, &comparison) &&
          comparison->comparison_type() == ComparisonType::kEqual &&
          SomeAttributeReference::MatchesWithConditionalCast(comparison->left(),
                                                             &attribute) &&
          SomeScalarLiteral::MatchesWithConditionalCast(comparison->right(),
                                                        &literal)) {
        collectConstant(node, attribute->id(), literal->value());
      }
    }
  }

  EquiJoinPtr equi_join;
  if (SomeEquiJoin::MatchesWithConditionalCast(node, &equi_join)) {
    const std::size_t num_join_attributes = equi_join->build_attributes().size();
    for (std::size_t i = 0; i < num_join_attributes; ++i) {
      const auto probe_attr = equi_join->probe_attributes().at(i)->id();
      const auto build_attr = equi_join->build_attributes().at(i)->id();
      const auto probe_it = constants_.find(probe_attr);
      const auto build_it = constants_.find(build_attr);

      if (probe_it != constants_.end()) {
        constants_.emplace(build_attr, probe_it->second);
      }
      if (build_it != constants_.end()) {
        constants_.emplace(probe_attr, build_it->second);
      }
    }
  }
}

void ConstantPropagation::collectConstant(const PlanPtr &source,
                                          const ExprId expr_id,
                                          const std::uint64_t value) {
  auto &source_memo = source_[source];

  // TODO(robin-team): Better to do "intersection" here.
  constants_.emplace(expr_id, value);
  source_memo.emplace(expr_id);

  // Try to infer constant values for other output attributes.
  const Attribute *key = cost_model_.findSourceAttribute(expr_id, source);
  if (key == nullptr) {
    return;
  }

  const Relation &relation = key->getParentRelation();
  if (relation.getNumBlocks() == 0) {
    return;
  }
  DCHECK_EQ(1u, relation.getNumBlocks());

  const IndexManager &index_manager = relation.getIndexManager();
  if (!index_manager.hasPrimaryKeyIndex(key->id())) {
    return;
  }

  const StorageBlock &block = relation.getBlock(0);

  const PrimaryKeyIndex &pk_index = index_manager.getPrimaryKeyIndex(key->id());
  const tuple_id tuple = pk_index.lookupVirtual(value);

  if (tuple == kInvalidTupleID) {
    constants_[expr_id] = kInvalidConstant;
    return;
  }

  for (const auto &attr : source->getOutputAttributes()) {
    const Attribute *target = cost_model_.findSourceAttribute(attr->id(), source);
    if (target == nullptr ||
        target->getParentRelation().getID() != relation.getID()) {
      continue;
    }

    constants_.emplace(attr->id(), block.getValueVirtual(target->id(), tuple));
    source_memo.emplace(attr->id());
  }
}

PlanPtr ConstantPropagation::attachFilters(
    const PlanPtr &node,
    std::unordered_set<ExprId> *filtered) {
  // First process child nodes
  std::vector<PlanPtr> new_children;
  for (const auto &child : node->children()) {
    std::unordered_set<ExprId> child_filtered;
    new_children.emplace_back(attachFilters(child, &child_filtered));
    InsertAll(child_filtered, filtered);
  }

  PlanPtr input;
  if (new_children == node->children()) {
    input = node;
  } else {
    input = node->copyWithNewChildren(new_children);
  }

  std::vector<PredicatePtr> filters;
  for (const auto &attr : input->getOutputAttributes()) {
    const ExprId id = attr->id();

    if (ContainsKey(source_[node], id)) {
      filtered->emplace(id);
      continue;
    }
    if (ContainsKey(*filtered, id)) {
      continue;
    }

    const auto it = constants_.find(id);
    if (it == constants_.end()) {
      continue;
    }
    if (it->second == kInvalidConstant) {
      filters = { PredicateLiteral::Create(false) };
      filtered->emplace(id);
      break;
    }

    const ScalarLiteralPtr literal =
        ScalarLiteral::Create(it->second,
                              UInt64Type::Instance(),
                              optimizer_context_->nextExprId());
    const ComparisonPtr comparison =
        Comparison::Create(ComparisonType::kEqual, attr, literal);

    filters.emplace_back(comparison);
    filtered->emplace(id);
  }

  if (filters.empty()) {
    return input;
  }

  return Selection::Create(
      input,
      CastSharedPtrVector<Scalar>(node->getOutputAttributes()),
      CreateConjunctivePredicate(filters));
}

}  // namespace optimizer
}  // namespace project

