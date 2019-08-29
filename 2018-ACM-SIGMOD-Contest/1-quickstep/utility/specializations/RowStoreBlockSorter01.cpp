#include "utility/specializations/RowStoreBlockSorter01.hpp"

#include "types/Type.hpp"
#include "utility/specializations/RowStoreBlockSorterCommon.hpp"

namespace project {

void SortRowStoreBlockX(Task *ctx,
                        const Type *t0,
                        RowStoreBlock *block) {
  InvokeOnTypeIDsForCppTypes(
      t0->getTypeID(),
      [&](auto typelist) -> void {
    SortRowStoreBlockImpl<decltype(typelist)>(ctx, block);
  });
}

void SortRowStoreBlockXX(Task *ctx,
                         const Type *t0,
                         const Type *t1,
                         RowStoreBlock *block) {
  InvokeOnTypeIDsForCppTypes(
      t0->getTypeID(), t1->getTypeID(),
      [&](auto typelist) -> void {
    SortRowStoreBlockImpl<decltype(typelist)>(ctx, block);
  });
}

}  // namespace project
