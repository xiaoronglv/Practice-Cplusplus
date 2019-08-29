#include "types/Type.hpp"

namespace project {

const Type& Type::GetInstance(const TypeID type_id) {
  return *InvokeOnTypeID(type_id, [](auto ic) -> const Type* {
    return &TypeIDTrait<decltype(ic)::value>::TypeClass::Instance();
  });
}

}  // namespace project
