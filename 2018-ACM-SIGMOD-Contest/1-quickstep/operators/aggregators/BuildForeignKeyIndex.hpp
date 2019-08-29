#ifndef PROJECT_OPERATORS_AGGREGATOR_BUILD_FOREIGN_KEY_INDEX_HPP_
#define PROJECT_OPERATORS_AGGREGATOR_BUILD_FOREIGN_KEY_INDEX_HPP_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "operators/utility/KeyCountVector.hpp"
#include "scheduler/Task.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/ForeignKeyIndex.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/MultiwayParallelMergeSort.hpp"
#include "utility/Range.hpp"
#include "utility/ScopedArray.hpp"

#include "glog/logging.h"

namespace project {

class BuildForeignKeyIndex {
 public:
  BuildForeignKeyIndex() {}

  template <typename Callback>
  static void InvokeWithCallback(Task *ctx,
                                 Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  static void BuildKeyCountVectorFromForeignKeySlots(
      Task *ctx, Relation *relation,
      const attribute_id id,
      const Range &domain,
      const std::size_t max_count,
      const std::pair<tuple_id, std::uint32_t> *slots);

  static constexpr std::size_t kBuildKeyCountVectorBatchSize = 1000000;

  DISALLOW_COPY_AND_ASSIGN(BuildForeignKeyIndex);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void BuildForeignKeyIndex::InvokeWithCallback(Task *ctx,
                                              Relation *relation,
                                              const attribute_id id,
                                              const Callback &callback) {
  DCHECK_EQ(1u, relation->getNumBlocks());

  const AttributeStatistics &stats = relation->getStatistics().getAttributeStatistics(id);
  DCHECK(stats.hasMinValue());
  DCHECK(stats.hasMaxValue());

  relation->forSingletonBlock(
      id,
      [&](auto accessor) {
    DCHECK_NE(0u, accessor->getNumTuples());

    const std::size_t num_tuples = accessor->getNumTuples();

    using T = typename std::remove_pointer_t<
        std::remove_const_t<decltype(accessor.get())>>::ValueType;
    using Bucket = std::pair<T, tuple_id>;

    Bucket *buckets = ScopedArray<Bucket>(num_tuples).release();
    for (std::size_t i = 0; i < num_tuples; ++i) {
      buckets[i].first = accessor->at(i);
      buckets[i].second = i;
    }

    const std::pair<T, tuple_id> guardian(std::numeric_limits<T>::max(),
                                          std::numeric_limits<tuple_id>::max());

    auto comparator = [](const auto &lhs, const auto &rhs) -> bool {
      // FIXME(robin-team): There is some problem with sorting -- fix it to
      // compare the first element only.
      return lhs < rhs;
    };

    MultiwayMergeSort(
        ctx, buckets, num_tuples, guardian, comparator,
        [&stats, num_tuples, buckets, relation, id, callback](Task *internal) {
      const std::size_t base = stats.getMinValue();
      const std::size_t range = stats.getMaxValue() - base + 1;

      ScopedArray<std::pair<tuple_id, std::uint32_t>> slots(range, true);
      for (std::size_t i = 0; i < num_tuples; ++i) {
        ++slots[buckets[i].first - base].second;
      }

      // Recalculate prefix sum and mark empty entries.
      std::uint32_t sum = 0;
      std::uint32_t max_count = 0;
      for (std::size_t i = 0; i < range; ++i) {
        const std::uint32_t count = slots[i].second;
        if (count == 0) {
          slots[i].first = kInvalidTupleID;
        } else {
          slots[i].first = sum;
          sum += count;
          max_count = std::max(max_count, count);
        }
      }

      BuildKeyCountVectorFromForeignKeySlots(
          internal, relation, id,
          Range(base, base + range), max_count, slots.get());

      auto fk_index = std::make_unique<ForeignKeyIndexImpl<T>>(
          base, range, max_count, slots.release(), buckets);

      callback(fk_index);
    });
  });
}

void BuildForeignKeyIndex::BuildKeyCountVectorFromForeignKeySlots(
    Task *ctx, Relation *relation,
    const attribute_id id,
    const Range &domain,
    const std::size_t max_count,
    const std::pair<tuple_id, std::uint32_t> *slots) {
  auto holder = std::make_shared<std::unique_ptr<KeyCountVector>>();

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([domain, max_count, slots, holder](Task *internal) {
        InvokeOnMaxCount(
            max_count, [&](auto typelist) {
          using CountType = typename decltype(typelist)::head;
          using KeyCountVectorType = KeyCountVectorImpl<CountType>;

          KeyCountVectorType *kcv = new KeyCountVectorType(domain);
          holder->reset(kcv);

          const std::size_t base = domain.begin();

          const RangeSplitter splitter =
              RangeSplitter::CreateWithPartitionLength(
                  0, domain.size(), kBuildKeyCountVectorBatchSize);

          for (const Range range : splitter) {
            internal->spawnLambdaTask([base, range, slots, kcv] {
              for (std::size_t i = range.begin(); i < range.end(); ++i) {
                kcv->increaseCount(base + i, slots[i].second);
              }
            });
          }
        });
      }),
      CreateLambdaTask([relation, id, holder] {
        relation->getIndexManagerMutable()
                ->setKeyCountVector(id, holder->release());
      })));
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATOR_BUILD_FOREIGN_KEY_INDEX_HPP_

