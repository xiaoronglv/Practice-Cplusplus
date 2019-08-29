#ifndef PROJECT_OPERATORS_HASH_JOIN_OPERATOR_HPP_
#define PROJECT_OPERATORS_HASH_JOIN_OPERATOR_HPP_

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/OperatorTypedefs.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/HashFilter.hpp"
#include "utility/HashTable.hpp"
#include "utility/Macros.hpp"

namespace project {

class StorageBlock;
class Task;
class Type;

class BasicHashJoinOperator : public RelationalOperator {
 public:
  BasicHashJoinOperator(
      const std::size_t query_id,
      const Relation &probe_relation,
      const Relation &build_relation,
      Relation *output_relation,
      const attribute_id probe_attribute,
      const attribute_id build_attribute,
      std::vector<attribute_id> &&project_attributes,
      std::vector<JoinSide> &&project_attribute_sides,
      std::unique_ptr<Predicate> &&probe_filter_predicate,
      std::unique_ptr<Predicate> &&build_filter_predicate);

  std::string getName() const override {
    return "HashJoinOperator";
  }

  const Relation& getProbeRelation() const {
    return probe_context_.relation;
  }

  const Relation& getBuildRelation() const {
    return build_context_.relation;
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

    const Relation &relation;
    const std::unique_ptr<Predicate> filter_predicate;
  };

  void buildHash(Task *ctx);
  void buildHashBlock(const std::size_t index);

  void createBuildContext(Task *ctx);

  template <typename CppTypeList, typename TupleIdContainer>
  void buildHashInternal(const std::vector<std::unique_ptr<ColumnAccessor>> &accessors,
                         const TupleIdContainer &build_input_tuple_ids);

  void probeHash(Task *ctx);
  void probeHashBlock(const StorageBlock &block);

  template <bool first_null, typename CppTypeList,
            typename ProbeAccessor, typename TupleIdContainer>
  void probeHashInternal(
      const ProbeAccessor &probe_accessor,
      const TupleIdContainer &probe_input_tuple_ids,
      OrderedTupleIdSequence *probe_output_tuples,
      std::vector<std::unique_ptr<ColumnVector>*> *build_output_columns);

  static std::vector<const Type*> GetTypes(const JoinContext &context);

  const std::size_t num_output_attributes_;

  Relation *output_relation_;

  // Intermediate data structures.
  // ---------------------------------------------------------------------------

  JoinContext probe_context_;
  JoinContext build_context_;

  std::vector<std::shared_ptr<const StorageBlock>> build_blocks_;
  std::vector<std::unique_ptr<TupleIdSequence>> build_filters_;
  std::atomic<std::size_t> build_side_num_tuples_;

  std::unique_ptr<HashFilter> hash_filter_;
  std::unique_ptr<HashTable> hash_table_;

  // TODO(robin-team): Adjust batch size with regard to dataset size.
  static constexpr std::size_t kBatchSize = 5000;

  DISALLOW_COPY_AND_ASSIGN(BasicHashJoinOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_HASH_JOIN_OPERATOR_HPP_
