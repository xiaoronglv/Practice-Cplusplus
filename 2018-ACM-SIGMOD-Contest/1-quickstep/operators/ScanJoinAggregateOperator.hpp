#ifndef PROJECT_OPERATORS_SCAN_JOIN_AGGREGATE_OPERATOR_HPP_
#define PROJECT_OPERATORS_SCAN_JOIN_AGGREGATE_OPERATOR_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "operators/OperatorTypedefs.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

namespace project {

class Relation;
class Task;

class FKPKScanJoinAggregateOperator : public RelationalOperator {
 public:
  FKPKScanJoinAggregateOperator(
      const std::size_t query_id,
      const Relation &probe_relation,
      const Relation &build_relation,
      Relation *output_relation,
      const attribute_id probe_attribute,
      const attribute_id build_attribute,
      std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
      std::vector<JoinSide> &&aggregate_expression_sides,
      std::unique_ptr<Predicate> &&probe_filter_predicate,
      std::unique_ptr<Predicate> &&build_filter_predicate);

  std::string getName() const override {
    return "FKPKScanJoinAggregateOperator";
  }

  const Relation& getProbeRelation() const {
    return probe_relation_;
  }

  const Relation& getBuildRelation() const {
    return build_relation_;
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  void executeBuildSidePKScan(Task *ctx);
  void executeProbeSide(Task *ctx);
  void executeProbeSideBlock(Task *ctx, const std::shared_ptr<const StorageBlock> &block);
  void finalize();

  const Relation &probe_relation_;
  const Relation &build_relation_;
  Relation *output_relation_;

  const attribute_id probe_attribute_;
  const attribute_id build_attribute_;

  const std::vector<std::unique_ptr<Scalar>> aggregate_expressions_;
  const std::vector<JoinSide> aggregate_expression_sides_;

  const std::unique_ptr<Predicate> probe_filter_predicate_;
  const std::unique_ptr<Predicate> build_filter_predicate_;

  // Intermediate data structures.
  // ---------------------------------------------------------------------------
  std::atomic<bool> is_null_;
  std::shared_ptr<std::uint64_t> build_attribute_literal_;
  std::vector<std::uint64_t> build_sums_base_;

  std::unique_ptr<Predicate> join_predicate_;

  std::vector<std::atomic<std::uint64_t>> sums_;

  std::vector<const Scalar*> probe_aggregate_expressions_;
  std::vector<std::atomic<std::uint64_t>*> probe_sums_;

  std::vector<const Scalar*> build_aggregate_expressions_;
  std::vector<std::atomic<std::uint64_t>*> build_sums_;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(FKPKScanJoinAggregateOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_SCAN_JOIN_AGGREGATE_OPERATOR_HPP_
