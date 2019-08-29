#ifndef PROJECT_OPERATORS_AGGREGATOR_AGGREGATION_MIN_MAX_HPP_
#define PROJECT_OPERATORS_AGGREGATOR_AGGREGATION_MIN_MAX_HPP_

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "scheduler/Task.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {

class AggregationMinMax {
 public:
  AggregationMinMax() {}

  template <typename Callback>
  static void InvokeWithCallback(Task *ctx,
                                 Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  template <typename Accessor>
  static void CalculateMinMaxLocal(const Accessor &accessor,
                                   std::atomic<std::uint64_t> *min_value,
                                   std::atomic<std::uint64_t> *max_value);

  static constexpr std::uint64_t kBatchSize = 1000000;

  DISALLOW_COPY_AND_ASSIGN(AggregationMinMax);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void AggregationMinMax::InvokeWithCallback(Task *ctx,
                                           Relation *relation,
                                           const attribute_id id,
                                           const Callback &callback) {
  auto min_value = std::make_shared<std::atomic<std::uint64_t>>(
      std::numeric_limits<std::uint64_t>::max());
  auto max_value = std::make_shared<std::atomic<std::uint64_t>>(
      std::numeric_limits<std::uint64_t>::min());

  ctx->spawnTask(CreateTaskChain(
    CreateLambdaTask([min_value, max_value, relation, id](Task *internal) {
      internal->setTaskName(new SimpleTaskName("minmax"));
      relation->forEachBlockPiece(
          id, kBatchSize,
          [&](auto accessor) {
        internal->spawnLambdaTask([min_value, max_value, accessor] {
          CalculateMinMaxLocal(*accessor, min_value.get(), max_value.get());
        });
      });
    }),
    CreateLambdaTask([min_value, max_value, callback] {
      callback(min_value->load(std::memory_order_relaxed),
               max_value->load(std::memory_order_relaxed));
    })));
}

template <typename Accessor>
void AggregationMinMax::CalculateMinMaxLocal(
    const Accessor &accessor,
    std::atomic<std::uint64_t> *min_value,
    std::atomic<std::uint64_t> *max_value) {
  const std::size_t num_tuples = accessor.getNumTuples();
  if (num_tuples == 0) {
    return;
  }

  using T = typename Accessor::ValueType;

  T local_min = std::numeric_limits<T>::max();
  T local_max = std::numeric_limits<T>::min();
  for (std::size_t i = 0; i < accessor.getNumTuples(); ++i) {
    const T value = accessor.at(i);
    local_min = std::min(local_min, value);
    local_max = std::max(local_max, value);
  }

  std::uint64_t value = min_value->load(std::memory_order_relaxed);
  while (!min_value->compare_exchange_weak(
              value, std::min(value, static_cast<std::uint64_t>(local_min)))) {}

  value = max_value->load(std::memory_order_relaxed);
  while (!max_value->compare_exchange_weak(
              value, std::max(value, static_cast<std::uint64_t>(local_max)))) {}
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATOR_AGGREGATION_MIN_MAX_HPP_
