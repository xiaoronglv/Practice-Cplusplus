#include "utility/specializations/RowStoreBlockSorter02.hpp"

#include "types/Type.hpp"
#include "utility/specializations/RowStoreBlockSorterCommon.hpp"

namespace project {

void SortRowStoreBlockXX0(Task *ctx,
                          const Type *t0,
                          const Type *t1,
                          RowStoreBlock *block) {
  InvokeOnTypeIDsForCppTypes(
      t0->getTypeID(), t1->getTypeID(),
      [&](auto typelist) -> void {
    using CppTypeList =
        typename decltype(typelist)::template push_back<std::uint32_t>;

    SortRowStoreBlockImpl<CppTypeList>(ctx, block);
  });
}

}  // namespace project
