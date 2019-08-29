#ifndef PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_01_HPP_
#define PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_01_HPP_

#include "types/Type.hpp"

namespace project {

class RowStoreBlock;
class Task;
class Type;

void SortRowStoreBlockX(Task *ctx,
                        const Type *t0,
                        RowStoreBlock *block);

void SortRowStoreBlockXX(Task *ctx,
                         const Type *t0,
                         const Type *t1,
                         RowStoreBlock *block);

}  // namespace project

#endif  // PROJECT_UTILITY_SPECIALIZATIONS_ROW_STORE_BLOCK_SORTER_01_HPP_
