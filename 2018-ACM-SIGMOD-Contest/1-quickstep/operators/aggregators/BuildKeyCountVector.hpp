#ifndef PROJECT_OPERATORS_AGGREGATORS_BUILD_KEY_COUNT_VECTOR_HPP_
#define PROJECT_OPERATORS_AGGREGATORS_BUILD_KEY_COUNT_VECTOR_HPP_

#include <cstddef>
#include <functional>
#include <limits>
#include <memory>

#include "operators/utility/KeyCountVector.hpp"
#include "partition/PartitionDestination.hpp"
#include "partition/PartitionDestinationBase.hpp"
#include "scheduler/Task.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/GenericAccessor.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/HashFilter.hpp"
#include "utility/Macros.hpp"

namespace project {

class ColumnAccessor;
class Range;
class TupleIdSequence;
class Type;

class KeyCountVectorBuilder {
 public:
  KeyCountVectorBuilder() {}

  virtual ~KeyCountVectorBuilder() {}

  static KeyCountVectorBuilder* Create(const Type &type,
                                       const std::size_t estimated_num_tuples,
                                       const Range &domain,
                                       const std::size_t max_count,
                                       const HashFilter *lookahead_filter,
                                       std::unique_ptr<KeyCountVector> *output);

  virtual void accumulate(Task *ctx,
                          const std::shared_ptr<const ColumnAccessor> &accessor,
                          const TupleIdSequence *filter) = 0;

  virtual void finalize(Task *ctx) = 0;

 private:
  static constexpr std::size_t kSingleThreadCardinalityThreshold = 1000000ul;
  static constexpr std::size_t kTwoPhaseDomainThreshold = 8192ul;

  DISALLOW_COPY_AND_ASSIGN(KeyCountVectorBuilder);
};


class BuildKeyCountVector {
 public:
  template <typename Callback>
  static void InvokeWithCallback(Task *ctx, Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  template <typename Callback>
  static void BuildWithBlocks(Task *ctx, Relation *relation,
                              const attribute_id id,
                              const Range &domain,
                              const Callback &callback);

  template <typename Callback>
  static void BuildWithPartitions(Task *ctx, Relation *relation,
                                  const attribute_id id,
                                  const Range &domain,
                                  const Callback &callback);

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(BuildKeyCountVector);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void BuildKeyCountVector::InvokeWithCallback(Task *ctx, Relation *relation,
                                             const attribute_id id,
                                             const Callback &callback) {
  const IndexManager &index_manager = relation->getIndexManager();
  if (index_manager.hasKeyCountVector(id)) {
    return;
  }

  const auto &stat = relation->getStatistics().getAttributeStatistics(id);
  DCHECK(stat.hasMinValue());
  DCHECK(stat.hasMaxValue());

  const Range domain(stat.getMinValue(), stat.getMaxValue() + 1);

  // TODO(robin-team): Utilize foreign key index if exists.
  if (index_manager.hasRangedPartitions(id)) {
    BuildWithPartitions(ctx, relation, id, domain, callback);
  } else {
    BuildWithBlocks(ctx, relation, id, domain, callback);
  }
}

template <typename Callback>
void BuildKeyCountVector::BuildWithBlocks(Task *ctx, Relation *relation,
                                          const attribute_id id,
                                          const Range &domain,
                                          const Callback &callback) {
  auto holder = std::make_shared<std::unique_ptr<KeyCountVector>>();

  std::shared_ptr<KeyCountVectorBuilder> builder(
      KeyCountVectorBuilder::Create(relation->getAttributeType(id),
                                    relation->getNumTuples(),
                                    domain,
                                    std::numeric_limits<std::uint32_t>::max(),
                                    nullptr /* lookahead_filter */,
                                    holder.get()));

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([relation, id, builder](Task *internal) {
        relation->forEachBlockPiece(
            id, kBatchSize,
            [&](auto accessor) {
          builder->accumulate(
              internal,
              std::static_pointer_cast<const ColumnAccessor>(accessor),
              nullptr /* filter */);
        });
      }),
      CreateLambdaTask([builder](Task *internal) {
        builder->finalize(internal);
      }),
      CreateLambdaTask([holder, builder, callback] {
        callback(*holder);
      })));
}

template <typename Callback>
void BuildKeyCountVector::BuildWithPartitions(Task *ctx, Relation *relation,
                                              const attribute_id id,
                                              const Range &domain,
                                              const Callback &callback) {
  const IndexManager &index_manager = relation->getIndexManager();
  DCHECK(index_manager.hasRangedPartitions(id));

  const PartitionDestinationBase &base = index_manager.getRangedPartitions(id);

  using KeyCountVectorType = KeyCountVectorImpl<std::uint32_t>;
  KeyCountVectorType *kcv = new KeyCountVectorType(domain);

  auto holder = std::make_shared<std::unique_ptr<KeyCountVector>>(kcv);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([relation, id, &base, kcv](Task *internal) {
        InvokeOnTypeIDForCppType(
            relation->getAttributeType(id).getTypeID(),
            [&](auto typelist) {
          using ValueType = typename decltype(typelist)::head;
          using PartitionDestinationType = PartitionDestination<ValueType>;

          const PartitionDestinationType &partitions =
              static_cast<const PartitionDestinationType&>(base);

          for (std::size_t i = 0; i < partitions.getNumPartitions(); ++i) {
            internal->spawnLambdaTask([i, &partitions, kcv] {
              partitions.forEach(i, [&](const auto &value) {
                kcv->increaseCount(value);
              });
            });
          }
        });
      }),
      CreateLambdaTask([holder, callback] {
        callback(*holder);
      })));
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATORS_BUILD_KEY_COUNT_VECTOR_HPP_
