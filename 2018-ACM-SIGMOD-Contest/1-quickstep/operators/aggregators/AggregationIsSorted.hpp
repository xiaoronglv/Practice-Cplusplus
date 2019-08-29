#ifndef PROJECT_OPERATORS_AGGREGATOR_AGGREGATION_IS_SORTED_HPP_
#define PROJECT_OPERATORS_AGGREGATOR_AGGREGATION_IS_SORTED_HPP_

#include <cstdint>
#include <limits>
#include <memory>

#include "scheduler/Task.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/Relation.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/StorageBlock.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class AggregationIsSorted {
 public:
  AggregationIsSorted() {}

  template <typename Callback>
  static void InvokeWithCallback(Task *ctx,
                                 Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  template <typename T, typename Callback>
  static void InvokeInternal(Relation *relation,
                             const attribute_id id,
                             const Callback &callback);

  template <typename T, typename ColumnAccessorT>
  static SortOrder InvokeLocal(const ColumnAccessorT *accessor);

  template <typename T, typename ColumnAccessorT>
  static bool CheckAscendantOrder(const ColumnAccessorT *accessor,
                                  const std::size_t num_tuples,
                                  const std::size_t start);

  template <typename T, typename ColumnAccessorT>
  static bool CheckDescendantOrder(const ColumnAccessorT *accessor,
                                  const std::size_t num_tuples,
                                  const std::size_t start);

  DISALLOW_COPY_AND_ASSIGN(AggregationIsSorted);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void AggregationIsSorted::InvokeWithCallback(Task *ctx,
                                             Relation *relation,
                                             const attribute_id id,
                                             const Callback &callback) {
  ctx->spawnLambdaTask([relation, id, callback] {
    InvokeOnTypeIDForCppType(
        relation->getAttributeType(id).getTypeID(),
        [&](auto ic) {
      using T = typename decltype(ic)::head;
      AggregationIsSorted::InvokeInternal<T>(relation, id, callback);
    });
  });
}

template <typename T, typename Callback>
void AggregationIsSorted::InvokeInternal(Relation *relation,
                                         const attribute_id id,
                                         const Callback &callback) {
  SortOrder sort_order = SortOrder::kUnknown;
  relation->forEachBlockEarlyTermination(
      id,
      [&](auto accessor) -> EarlyTerminationStatus {
    const SortOrder block_sort_order = InvokeLocal<T>(accessor.get());
    if (sort_order == SortOrder::kUnknown) {
      sort_order = block_sort_order;
    } else if (sort_order != block_sort_order) {
      sort_order = SortOrder::kNotSorted;
    }
    return sort_order == SortOrder::kNotSorted ? EarlyTerminationStatus::kYes
                                               : EarlyTerminationStatus::kNo;
  });
  callback(sort_order);
}

template <typename T, typename ColumnAccessorT>
SortOrder AggregationIsSorted::InvokeLocal(const ColumnAccessorT *accessor) {
  const std::size_t num_tuples = accessor->getNumTuples();
  if (num_tuples <= 1) {
    return SortOrder::kAscendant;
  }

  std::size_t pos = 1;
  while (pos < num_tuples && accessor->at(pos-1) == accessor->at(pos)) {
    ++pos;
  }

  if (pos == num_tuples) {
    return SortOrder::kAscendant;
  }

  if (accessor->at(pos-1) < accessor->at(pos)) {
    if (CheckAscendantOrder<T>(accessor, num_tuples, pos)) {
      return SortOrder::kAscendant;
    }
  } else {
    if (CheckDescendantOrder<T>(accessor, num_tuples, pos)) {
      return SortOrder::kDescendant;
    }
  }
  return SortOrder::kNotSorted;
}

template <typename T, typename ColumnAccessorT>
bool AggregationIsSorted::CheckAscendantOrder(const ColumnAccessorT *accessor,
                                              const std::size_t num_tuples,
                                              const std::size_t start) {
  for (std::size_t i = start+1; i < num_tuples; ++i) {
    if (accessor->at(i-1) > accessor->at(i)) {
      return false;
    }
  }
  return true;
}

template <typename T, typename ColumnAccessorT>
bool AggregationIsSorted::CheckDescendantOrder(const ColumnAccessorT *accessor,
                                               const std::size_t num_tuples,
                                               const std::size_t start) {
  for (std::size_t i = start+1; i < num_tuples; ++i) {
    if (accessor->at(i-1) < accessor->at(i)) {
      return false;
    }
  }
  return true;
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATOR_AGGREGATION_IS_SORTED_HPP_
