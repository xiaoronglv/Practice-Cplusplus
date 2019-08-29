#ifndef PROJECT_OPERATORS_AGGREGATOR_BUILD_PRIMARY_KEY_INDEX_HPP_
#define PROJECT_OPERATORS_AGGREGATOR_BUILD_PRIMARY_KEY_INDEX_HPP_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>

#include "partition/PartitionDestination.hpp"
#include "partition/PartitionDestinationBase.hpp"
#include "scheduler/Task.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/IndexManager.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedArray.hpp"

#include "glog/logging.h"

namespace project {

class BuildPrimaryKeyIndex {
 public:
  BuildPrimaryKeyIndex() {}

  template <typename Callback>
  static void InvokeWithCallback(Task *ctx, Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  static void BuildWithSingleThread(Relation *relation,
                                    const attribute_id id,
                                    const std::size_t base,
                                    ScopedArray<tuple_id> *slots);

  static void BuildParallel(Task *ctx, Relation *relation,
                            const attribute_id id,
                            const std::size_t base,
                            ScopedArray<tuple_id> *slots);

  static constexpr std::size_t kParallelBuildCardinalityThreshold = 1000000;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(BuildPrimaryKeyIndex);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void BuildPrimaryKeyIndex::InvokeWithCallback(Task *ctx,
                                              Relation *relation,
                                              const attribute_id id,
                                              const Callback &callback) {
  CHECK_EQ(1u, relation->getNumBlocks());

  const AttributeStatistics &stats =
      relation->getStatistics().getAttributeStatistics(id);

  DCHECK(stats.hasMinValue());
  DCHECK(stats.hasMaxValue());

  const std::size_t base = stats.getMinValue();
  const std::size_t range = stats.getMaxValue() - base + 1;

  auto slots = std::make_shared<ScopedArray<tuple_id>>(range);
  std::memset(slots->get(), 0xff, range * sizeof(tuple_id));

  ctx->spawnTask(CreateTaskChain(
    CreateLambdaTask([relation, id, base, slots](Task *internal) {
      if (relation->getNumTuples() <= kParallelBuildCardinalityThreshold) {
        BuildWithSingleThread(relation, id, base, slots.get());
      } else {
        BuildParallel(internal, relation, id, base, slots.get());
      }
    }),
    CreateLambdaTask([base, range, slots, relation, id, callback] {
      InvokeOnColumnAccessorMakeShared(
          relation->getBlock(0).createColumnAccessor(id),
          [&](auto accessor) {
        using T = typename std::remove_pointer_t<
            std::remove_const_t<decltype(accessor.get())>>::ValueType;

        auto pk_index = std::make_unique<PrimaryKeyIndexImpl<T>>(
            base, range, std::move(*slots), accessor->getData());

        callback(pk_index);
      });
    })));
}

void BuildPrimaryKeyIndex::BuildWithSingleThread(Relation *relation,
                                                 const attribute_id id,
                                                 const std::size_t base,
                                                 ScopedArray<tuple_id> *slots) {
  relation->forEachBlock(
      id, [&](auto accessor) {
    for (std::size_t i = 0; i < accessor->getNumTuples(); ++i) {
      (*slots)[accessor->at(i) - base] = i;
    }
  });
}

void BuildPrimaryKeyIndex::BuildParallel(Task *ctx, Relation *relation,
                                         const attribute_id id,
                                         const std::size_t base,
                                         ScopedArray<tuple_id> *slots) {
  std::size_t offset = 0;
  relation->forEachBlockPiece(
      id, kBatchSize,
      [&](auto accessor) {
    const std::size_t num_tuples = accessor->getNumTuples();
    ctx->spawnLambdaTask([accessor, slots, base, offset, num_tuples] {
      for (std::size_t i = 0; i < num_tuples; ++i) {
        (*slots)[accessor->at(i) - base] = offset + i;
      }
    });
    offset += num_tuples;
  });
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATOR_BUILD_PRIMARY_KEY_INDEX_HPP_

