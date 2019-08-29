#ifndef PROJECT_OPERATORS_UTILITY_MULTIWAY_JOIN_CONTEXT_HPP_
#define PROJECT_OPERATORS_UTILITY_MULTIWAY_JOIN_CONTEXT_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Predicate.hpp"
#include "operators/utility/KeyCountVector.hpp"
#include "scheduler/Task.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/HashFilter.hpp"

namespace project {

class Scalar;
class StorageBlock;

struct MultiwayJoinContext {
  MultiwayJoinContext(const Relation &relation_in,
              const attribute_id join_attribute_in,
              std::unique_ptr<Predicate> &&filter_predicate_in,
              const std::uint32_t max_count_in)
      : total_num_tuples(0),
        scan_blocks(true),
        build_key_count_vector(true),
        relation(relation_in),
        join_attribute(join_attribute_in),
        filter_predicate(std::move(filter_predicate_in)),
        max_count(max_count_in) {}

  std::vector<std::shared_ptr<const StorageBlock>> blocks;
  std::vector<std::unique_ptr<TupleIdSequence>> filters;
  std::unique_ptr<KeyCountVector> count_vector;

  std::atomic<std::size_t> total_num_tuples;
  // TODO(robin-team): Use two phase approach for small filters.
  std::unique_ptr<ConcurrentHashFilter> lookahead_filter;

  std::vector<const Scalar*> aggregate_expressions;
  std::vector<std::atomic<std::uint64_t>*> sums;

  bool scan_blocks;
  bool build_key_count_vector;

  const Relation &relation;
  const attribute_id join_attribute;
  const std::unique_ptr<Predicate> filter_predicate;
  const std::uint32_t max_count;
};

}  // namespace project

#endif  // PROJECT_OPERATORS_UTILITY_MULTIWAY_JOIN_CONTEXT_HPP_
