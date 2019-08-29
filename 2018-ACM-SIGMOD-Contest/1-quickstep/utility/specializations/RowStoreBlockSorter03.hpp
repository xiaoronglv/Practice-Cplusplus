#ifndef PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_03_HPP_
#define PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_03_HPP_

#include "types/Type.hpp"

namespace project {

class RowStoreBlock;
class Task;
class Type;

void SortRowStoreBlockXX1(Task *ctx,
                          const Type *t0,
                          const Type *t1,
                          RowStoreBlock *block);

}  // namespace project

#endif  // PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_03_HPP_
