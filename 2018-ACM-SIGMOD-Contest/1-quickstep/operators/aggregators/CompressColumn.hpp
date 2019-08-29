#ifndef PROJECT_OPERATORS_AGGREGATOR_COMPRESS_COLUMN_HPP_
#define PROJECT_OPERATORS_AGGREGATOR_COMPRESS_COLUMN_HPP_

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "scheduler/Task.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/Relation.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class CompressColumn {
 public:
  CompressColumn() {}

  template <typename Callback>
  static void InvokeWithCallback(Task *ctx,
                                 Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  using ColumnVectorType = ColumnVectorImpl<std::uint32_t>;

  template <typename Accessor>
  static void CompressInternal(const Accessor &accessor,
                               const std::size_t offset,
                               ColumnVectorType *output,
                               std::atomic<std::uint64_t> *min_value,
                               std::atomic<std::uint64_t> *max_value);

  static constexpr std::uint64_t kBatchSize = 200000;

  DISALLOW_COPY_AND_ASSIGN(CompressColumn);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void CompressColumn::InvokeWithCallback(Task *ctx,
                                        Relation *relation,
                                        const attribute_id id,
                                        const Callback &callback) {
  auto keeper = std::make_shared<std::unique_ptr<ColumnVectorType>>();
  auto &output = *keeper;

  const std::size_t num_tuples = relation->getNumTuples();
  output = std::make_unique<ColumnVectorType>(num_tuples);
  output->resize(num_tuples);

  auto min_value = std::make_shared<std::atomic<std::uint64_t>>(
      std::numeric_limits<std::uint64_t>::max());
  auto max_value = std::make_shared<std::atomic<std::uint64_t>>(
      std::numeric_limits<std::uint64_t>::min());

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([relation, id, &output,
                        min_value, max_value](Task *internal) {
        std::size_t offset = 0;
        relation->forEachBlockPiece(
            id, kBatchSize,
            [&](auto accessor) {
          internal->spawnLambdaTask([accessor, offset, &output,
                                     min_value, max_value] {
            CompressInternal(*accessor,
                             offset,
                             output.get(),
                             min_value.get(),
                             max_value.get());
          });
          offset += accessor->getNumTuples();
        });
      }),
      CreateLambdaTask([keeper, min_value, max_value, callback] {
        callback(*keeper, UInt32Type::Instance(),
                 min_value->load(std::memory_order_relaxed),
                 max_value->load(std::memory_order_relaxed));
      })));
}

template <typename Accessor>
void CompressColumn::CompressInternal(const Accessor &accessor,
                                      const std::size_t offset,
                                      ColumnVectorType *output,
                                      std::atomic<std::uint64_t> *min_value,
                                      std::atomic<std::uint64_t> *max_value) {
  DCHECK_NE(accessor.getNumTuples(), 0);

  std::uint64_t local_min = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t local_max = std::numeric_limits<std::uint64_t>::min();

  for (std::size_t i = 0; i < accessor.getNumTuples(); ++i) {
    const std::uint64_t value = accessor.at(i);

    local_min = std::min(local_min, value);
    local_max = std::max(local_max, value);

    output->setValue(offset + i, value);
  }

  std::uint64_t value;

  value = min_value->load(std::memory_order_relaxed);
  while (!min_value->compare_exchange_weak(
              value, std::min(value, static_cast<std::uint64_t>(local_min)))) {}

  value = max_value->load(std::memory_order_relaxed);
  while (!max_value->compare_exchange_weak(
              value, std::max(value, static_cast<std::uint64_t>(local_max)))) {}
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATOR_COMPRESS_COLUMN_HPP_
