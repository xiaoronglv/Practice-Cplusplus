#include "utility/RowStoreBlockSorter.hpp"

#include <vector>

#include "storage/RowStoreBlock.hpp"
#include "types/Type.hpp"
#include "utility/specializations/RowStoreBlockSorter01.hpp"
#include "utility/specializations/RowStoreBlockSorter02.hpp"
#include "utility/specializations/RowStoreBlockSorter03.hpp"

#include "glog/logging.h"

namespace project {

void SortRowStoreBlock(Task *ctx,
                       const std::vector<const Type*> &types,
                       RowStoreBlock *block) {
  switch (types.size()) {
    case 1u:
      SortRowStoreBlockX(ctx, types[0], block);
      break;
    case 2u:
      SortRowStoreBlockXX(ctx, types[0], types[1], block);
      break;
    case 3u:
      switch (types[2]->getTypeID()) {
        case kUInt32:
          SortRowStoreBlockXX0(ctx, types[0], types[1], block);
          break;
        case kUInt64:
          SortRowStoreBlockXX1(ctx, types[0], types[1], block);
          break;
      }
      break;
    default:
      LOG(FATAL) << "Unsupported number of types";;
  }
}

}  // namespace project
