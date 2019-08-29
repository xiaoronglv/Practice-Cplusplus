#ifndef PROJECT_OPERATORS_CHAINED_JOIN_AGGREGATE_OPERATOR_HPP_
#define PROJECT_OPERATORS_CHAINED_JOIN_AGGREGATE_OPERATOR_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/utility/KeyCountVector.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {

class Relation;
class StorageBlock;
class Task;

class ChainedJoinAggregateOperator : public RelationalOperator {
 public:
  ChainedJoinAggregateOperator(
      const std::size_t query_id,
      const std::vector<const Relation*> &input_relations,
      Relation *output_relation,
      const std::vector<std::pair<attribute_id, attribute_id>> &join_attribute_pairs,
      std::vector<Range> &&join_attribute_ranges,
      std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
      std::vector<std::unique_ptr<Predicate>> &&filter_predicates);

  std::string getName() const override {
    return "ChainedJoinAggregateOperator";
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  struct JoinContext {
    JoinContext(const Relation &relation_in,
                const attribute_id build_attribute_in,
                const attribute_id probe_attribute_in,
                const Range &domain_in,
                std::unique_ptr<Predicate> &&filter_predicate_in)
        : total_num_tuples(0),
          relation(relation_in),
          build_attribute(build_attribute_in),
          probe_attribute(probe_attribute_in),
          domain(domain_in),
          filter_predicate(std::move(filter_predicate_in)) {}

    std::vector<std::shared_ptr<const StorageBlock>> blocks;
    std::vector<std::unique_ptr<TupleIdSequence>> filters;
    std::unique_ptr<KeyCountVector> count_vector;

    std::atomic<std::size_t> total_num_tuples;

    const Relation &relation;
    const attribute_id build_attribute;
    const attribute_id probe_attribute;
    const Range domain;
    const std::unique_ptr<Predicate> filter_predicate;
  };

  void createFirstCountVector(Task *ctx);
  bool tryCreateFirstCountVectorFromIndex();

  void executeChainJoins(Task *ctx);
  void executeChainJoin(Task *ctx, const std::size_t index);

  template <bool input_atomic>
  void accumulateCountVectorInChain(Task *ctx, const std::size_t index);

  template <typename InputCountVector, typename OutputCountVector>
  void accumulateCountVectorDispatch(Task *ctx, JoinContext *context,
                                     const InputCountVector &input_kcv,
                                     OutputCountVector *output_kcv);

  template <typename BuildAccessor, typename ProbeAccessor,
            typename TupleIdContainer,
            typename InputCountVector, typename OutputCountVector>
  void accumulateCountVectorBlock(const Range &build_domain,
                                  const BuildAccessor &build_accessor,
                                  const ProbeAccessor &probe_accessor,
                                  const TupleIdContainer &tuple_ids,
                                  const InputCountVector &input_kcv,
                                  OutputCountVector *output_kcv);

  template <typename Accessor, typename TupleIdContainer, typename CountVector>
  void accumulateOutputBlock(const StorageBlock &block,
                             const Accessor &accessor,
                             const TupleIdContainer &tuple_ids,
                             const CountVector &cv);

  void accumulateOutputDispatch(Task *ctx);

  void accumulateOutput(Task *ctx);

  void createJoinContext(Task *ctx, JoinContext *context);
  void createFilter(const std::size_t index, JoinContext *context);

  void finalize();

  Relation *output_relation_;
  const std::vector<std::unique_ptr<Scalar>> aggregate_expressions_;

  std::vector<std::unique_ptr<JoinContext>> contexts_;

  std::atomic<bool> is_null_;
  std::vector<std::atomic<std::uint64_t>> sums_;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(ChainedJoinAggregateOperator);
};

}  // namespace project

#endif   // PROJECT_OPERATORS_CHAINED_JOIN_AGGREGATE_OPERATOR_HPP_
