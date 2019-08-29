#ifndef PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_COMMON_HPP_
#define PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_COMMON_HPP_

#include <algorithm>
#include <cstring>
#include <limits>
#include <tuple>
#include <type_traits>

#include "scheduler/Task.hpp"
#include "storage/RowStoreBlock.hpp"
#include "types/Type.hpp"
#include "utility/MultiwayParallelMergeSort.hpp"

namespace project {

template <typename CppTypeList>
inline void SortRowStoreBlockImpl(Task *ctx, RowStoreBlock *block) {
  using RowStoreBlockType =
      typename CppTypeList::template bind_to<RowStoreBlockImpl>;

  RowStoreBlockType *row_block = static_cast<RowStoreBlockType*>(block);

  using RowType = typename RowStoreBlockType::Row;

  RowType guardian;
  std::get<0>(guardian) = std::numeric_limits<typename CppTypeList::head>::max();

  MultiwayMergeSort(
     ctx, row_block->dataMutable(), row_block->getNumTuples(), guardian,
     [](const auto &lhs, const auto &rhs) -> bool {
    // FIXME(robin-team): There is some problem with sorting -- fix it to
    // compare the first element only.
    // return std::get<0>(lhs) < std::get<0>(rhs);
    return lhs < rhs;
  });

//  std::sort(row_block->dataMutable(),
//            row_block->dataMutable() + row_block->getNumTuples(),
//            [](const auto &lhs, const auto &rhs) -> bool {
//    return std::get<0>(lhs) < std::get<0>(rhs);
//  });
}

}  // namespace project

#endif  // PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_COMMON_HPP_

