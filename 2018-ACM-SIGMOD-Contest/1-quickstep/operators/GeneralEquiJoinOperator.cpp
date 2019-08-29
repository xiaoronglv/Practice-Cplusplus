#include "operators/GeneralEquiJoinOperator.hpp"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Predicate.hpp"
#include "scheduler/Task.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"

#include "glog/logging.h"

namespace project {

template <typename JoinOperator, typename ...Args>
void GeneralEquiJoinOperator::initialize(std::unique_ptr<JoinOperator> *join,
                                         Args &&...args) {
  *join = std::make_unique<JoinOperator>(std::forward<Args>(args)...);
}

template <typename ...Args>
void GeneralEquiJoinOperator::initialize(const JoinOperatorType type,
                                         Args &&...args) {
  join_type_ = std::make_unique<JoinOperatorType>(type);

  switch (type) {
    case kBasicHashJoin:
      initialize(&basic_hash_join_, std::forward<Args>(args)...);
      break;
    case kSortMergeJoin:
      initialize(&sort_merge_join_, std::forward<Args>(args)...);
      break;
    default:
      LOG(FATAL) << "Unsupported GeneralEquiJoinOperator type";
  }
}

GeneralEquiJoinOperator::GeneralEquiJoinOperator(
    const std::size_t query_id,
    const Relation *probe_relation,
    const Relation *build_relation,
    Relation *output_relation,
    attribute_id probe_attribute,
    attribute_id build_attribute,
    std::vector<attribute_id> &&project_attributes,
    std::vector<JoinSide> &&project_attibute_sides,
    std::unique_ptr<Predicate> &&probe_filter_predicate,
    std::unique_ptr<Predicate> &&build_filter_predicate)
    : RelationalOperator(query_id) {
  JoinOperatorType type = kSortMergeJoin;

  const std::uint64_t probe_cardinality = probe_relation->getNumTuples();
  const std::uint64_t build_cardinality = build_relation->getNumTuples();

  if (probe_cardinality < kBasicHashJoinCardinalityThreshold ||
      build_cardinality < kBasicHashJoinCardinalityThreshold) {
    // TODO(robin-team): We might further check uniqueness of join attributes
    // to specialize the basic hash join algorithm.
    if (probe_cardinality < build_attribute) {
      swapSide(&probe_relation, &build_relation,
               &probe_attribute, &build_attribute,
               &project_attibute_sides,
               &probe_filter_predicate, &build_filter_predicate);
    }
    type = kBasicHashJoin;
  }

  initialize(type, query_id,
             *probe_relation, *build_relation, output_relation,
             probe_attribute, build_attribute,
             std::move(project_attributes), std::move(project_attibute_sides),
             std::move(probe_filter_predicate), std::move(build_filter_predicate));
}

void GeneralEquiJoinOperator::execute(Task *ctx) {
  DCHECK(join_type_ != nullptr);
  switch (*join_type_) {
    case kBasicHashJoin:
      basic_hash_join_->execute(ctx);
      break;
    case kSortMergeJoin:
      sort_merge_join_->execute(ctx);
      break;
    default:
      LOG(FATAL) << "Unsupported GeneralEquiJoinOperator type";
  }
}

void GeneralEquiJoinOperator::swapSide(
    const Relation **probe_relation,
    const Relation **build_relation,
    attribute_id *probe_attribute,
    attribute_id *build_attribute,
    std::vector<JoinSide> *project_attibute_sides,
    std::unique_ptr<Predicate> *probe_filter_predicate,
    std::unique_ptr<Predicate> *build_filter_predicate) {
  std::swap(*probe_relation, *build_relation);
  std::swap(*probe_attribute, *build_attribute);
  std::swap(*probe_filter_predicate, *build_filter_predicate);
  for (std::size_t i = 0; i < project_attibute_sides->size(); ++i) {
    const JoinSide side = (*project_attibute_sides)[i];
    (*project_attibute_sides)[i] =
        (side == JoinSide::kProbe ? JoinSide::kBuild : JoinSide::kProbe);
  }
}

}  // namespace project
