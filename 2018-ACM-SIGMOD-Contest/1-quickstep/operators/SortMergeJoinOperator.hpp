#ifndef PROJECT_OPERATORS_SORT_MERGE_JOIN_OPERATOR_HPP_
#define PROJECT_OPERATORS_SORT_MERGE_JOIN_OPERATOR_HPP_

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/OperatorTypedefs.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/Relation.hpp"
#include "storage/RowStoreBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {

class StorageBlock;
class Task;
class Type;

class SortMergeJoinOperator : public RelationalOperator {
 public:
  SortMergeJoinOperator(
      const std::size_t query_id,
      const Relation &probe_relation,
      const Relation &build_relation,
      Relation *output_relation,
      const attribute_id probe_attribute,
      const attribute_id build_attribute,
      std::vector<attribute_id> &&project_attributes,
      std::vector<JoinSide> &&project_attibute_sides,
      std::unique_ptr<Predicate> &&probe_filter_predicate,
      std::unique_ptr<Predicate> &&build_filter_predicate);

  std::string getName() const override {
    return "SortMergeJoinOperator";
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  struct JoinContext {
    JoinContext(const Relation &relation_in,
                std::unique_ptr<Predicate> &&filter_predicate_in)
        : relation(relation_in),
          filter_predicate(std::move(filter_predicate_in)) {}

    // The first attribute is always the join attribute.
    std::vector<attribute_id> attributes;

    // If the join attribute does not belong to the output, then the first
    // element is -1.
    std::vector<int> output_column_indexes;

    std::unique_ptr<RowStoreBlock> buffer;

    const Relation &relation;
    const std::unique_ptr<Predicate> filter_predicate;
  };

  void collectSort(Task *ctx, JoinContext *context);

  void collectBlock(const StorageBlock &block,
                    std::unique_ptr<RowStoreBlock> *output,
                    JoinContext *context);

  void sort(Task *ctx, JoinContext *context);

  void mergeHelper(Task *ctx);

  // Full specialization on both sides of RowStoreBlock's incurs long compile
  // time. So here uses an alternative approach. But we may still try the full
  // specialization if necessary -- just manually split the workload into
  // multiple files.
  template <typename ProbeCppType, typename BuildCppType>
  void merge(const Range &range);

  void materializeHelper(const JoinContext &context,
                         const OrderedTupleIdSequence &tuples,
                         std::vector<std::unique_ptr<ColumnVector>> *output_columns);

  template <bool first_null, typename RowStoreBlockType>
  void materialize(const RowStoreBlockType &block,
                   const OrderedTupleIdSequence &tuples,
                   const std::vector<attribute_id> &attributes,
                   std::vector<std::unique_ptr<ColumnVector>*> *output_columns);

  static std::vector<const Type*> GetTypes(const JoinContext &context);

  const std::size_t num_output_attributes_;

  Relation *output_relation_;

  JoinContext probe_context_;
  JoinContext build_context_;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(SortMergeJoinOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_SORT_MERGE_JOIN_OPERATOR_HPP_
