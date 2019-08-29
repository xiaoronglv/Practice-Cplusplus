#ifndef PROJECT_UTILITY_ROW_STORE_BLOCK_SORTER_HPP_
#define PROJECT_UTILITY_ROW_STORE_BLOCK_SORTER_HPP_

#include <vector>

namespace project {

class RowStoreBlock;
class Task;
class Type;

// For reducing overall compile time ...
void SortRowStoreBlock(Task *ctx,
                       const std::vector<const Type*> &types,
                       RowStoreBlock *block);

}  // namespace project

#endif  // PROJECT_UTILITY_ROW_STORE_BLOCK_SORTER_HPP_
