#ifndef PROJECT_OPERATORS_MULTIWAY_JOIN_AGGREGATE_OPERATOR_HPP_
#define PROJECT_OPERATORS_MULTIWAY_JOIN_AGGREGATE_OPERATOR_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/OperatorTypedefs.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/utility/MultiwayJoinContext.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/HashFilter.hpp"

namespace project {

class Database;
class StorageBlock;
class Task;

class MultiwaySymmetricJoinAggregateOperator : public RelationalOperator {
 public:
  MultiwaySymmetricJoinAggregateOperator(
      const std::size_t query_id,
      const std::vector<const Relation*> &input_relations,
      Relation *output_relation,
      const Database &database,
      const std::vector<attribute_id> &join_attributes,
      const std::vector<std::uint32_t> &max_counts,
      const Range &join_attribute_range,
      std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
      std::vector<std::size_t> &&aggregate_expression_indexes,
      std::vector<std::unique_ptr<Predicate>> &&filter_predicates);

  std::string getName() const override {
    return "MultiwaySymmetricJoinAggregateOperator";
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  using JoinContext = MultiwayJoinContext;

  void eliminateRedundantKeyCountVectors(const Database &database);
  void eliminateRedundantBlockScan();

  void createJoinContext(Task *ctx, JoinContext *context);
  void createFilter(const std::size_t index, JoinContext *context);

  bool tryCreateCountVectorFromIndex(JoinContext *context);
  void createCountVector(Task *ctx, JoinContext *context);

  template <typename Accessor, typename TupleIdContainer>
  void createLookaheadFilterInternal(const Accessor &accessor,
                                     const TupleIdContainer &tuple_ids,
                                     ConcurrentHashFilter *hash_filter);

  void createLookaheadFilter(Task *ctx);
  void createLookaheadFilterContext(Task *ctx,
                                    JoinContext *context,
                                    const std::size_t cardinality);

  void mergeLookaheadFilters(Task *ctx);

  void finalize();

  Relation *output_relation_;
  const Range join_attribute_range_;

  const std::vector<std::unique_ptr<Scalar>> aggregate_expressions_;
  std::vector<std::unique_ptr<JoinContext>> contexts_;

  std::atomic<bool> has_empty_input_;
  std::unique_ptr<HashFilter> lookahead_filter_;

  std::atomic<bool> is_null_;
  std::vector<std::atomic<std::uint64_t>> sums_;

  static constexpr std::size_t kBatchSize = 100000;

  static constexpr std::size_t kLookaheadFilterCardinalityThreshold = 1024;
  static constexpr std::size_t kLookaheadFilterBoostFactor = 16;

  static constexpr std::size_t kParallelKeyCountVectorBuildThreshold = 1000000;

  static constexpr std::size_t kKeyCountVectorScanThreshold = 5000000;

  DISALLOW_COPY_AND_ASSIGN(MultiwaySymmetricJoinAggregateOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_MULTIWAY_JOIN_AGGREGATE_OPERATOR_HPP_
