#ifndef PROJECT_TYPES_TYPE_HPP_
#define PROJECT_TYPES_TYPE_HPP_

#include <cstdint>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "utility/Macros.hpp"
#include "utility/meta/TypeList.hpp"

#include "glog/logging.h"

namespace project {

enum TypeID {
  kUInt64 = 0,
  kUInt32
};

template <TypeID type_id>
struct TypeIDTrait;

#define REGISTER_TYPE(type_class, type_id, cpp_type) \
  class type_class; \
  template <> struct TypeIDTrait<type_id> { \
    typedef type_class TypeClass; \
    typedef cpp_type CppType; \
    static constexpr TypeID kStaticTypeID = type_id; \
  };

REGISTER_TYPE(UInt64Type, kUInt64, std::uint64_t);
REGISTER_TYPE(UInt32Type, kUInt32, std::uint32_t);

#undef REGISTER_TYPE

class Type {
 public:
  TypeID getTypeID() const {
    return type_id_;
  }

  virtual std::string getName() const = 0;

  static const Type& GetInstance(const TypeID type_id);

 protected:
  explicit Type(const TypeID type_id)
      : type_id_(type_id) {}

  const TypeID type_id_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Type);
};


template <TypeID type_id>
class UIntSuperType : public Type {
 private:
  using Trait = TypeIDTrait<type_id>;
  using TypeClass = typename Trait::TypeClass;

 public:
  static const TypeClass& Instance() {
    static TypeClass instance;
    return instance;
  }

 protected:
  UIntSuperType() : Type(type_id) {}

 private:
  DISALLOW_COPY_AND_ASSIGN(UIntSuperType);
};

class UInt64Type : public UIntSuperType<kUInt64> {
 public:
  std::string getName() const override {
    return "UInt64";
  }
};

class UInt32Type : public UIntSuperType<kUInt32> {
 public:
  std::string getName() const override {
    return "UInt32";
  }
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Functor>
inline auto InvokeOnTypeID(const TypeID type_id,
                           const Functor &functor) {
  switch (type_id) {
    case kUInt64:
      return functor(std::integral_constant<TypeID, kUInt64>());
    case kUInt32:
      return functor(std::integral_constant<TypeID, kUInt32>());
    default:
      break;
  }
  LOG(FATAL) << "Unexpected TypeID at InvokeOnTypeID()";
}

template <typename Functor>
inline auto InvokeOnTypeIDForCppType(const TypeID type_id,
                                     const Functor &functor) {
  return InvokeOnTypeID(type_id, [&](auto ic) {
    using CppType = typename TypeIDTrait<decltype(ic)::value>::CppType;
    return functor(meta::TypeList<CppType>());
  });
}

namespace internal {

template <std::size_t i, typename TypeIDList, typename TupleT, typename Functor>
inline auto InvokeOnTypeIDsImpl(
    const TupleT &args, const Functor &functor,
    std::enable_if_t<i+1 == std::tuple_size<TupleT>::value> * = 0) {
  return functor(TypeIDList());
}

template <std::size_t i, typename TypeIDList, typename TupleT, typename Functor>
inline auto InvokeOnTypeIDsImpl(
    const TupleT &args, const Functor &functor,
    std::enable_if_t<i+1 != std::tuple_size<TupleT>::value> * = 0) {
  return InvokeOnTypeID(
      std::get<i>(args), [&](auto ic) {
    using NextTypeIDList = typename TypeIDList::template push_back<decltype(ic)>;
    return InvokeOnTypeIDsImpl<i+1, NextTypeIDList>(args, functor);
  });
}

template <typename TypeIDConstant>
struct GetCppTypeForTypeID {
  using type = typename TypeIDTrait<TypeIDConstant::value>::CppType;
};

}  // namespace internal

template <typename ...ArgTypes>
inline auto InvokeOnTypeIDs(ArgTypes &&...args) {
  constexpr std::size_t last = sizeof...(ArgTypes) - 1;
  const auto &functor = std::get<last>(std::forward_as_tuple(args...));

  return internal::InvokeOnTypeIDsImpl<0, meta::TypeList<>>(
      std::forward_as_tuple(args...), functor);
}

template <typename ...ArgTypes>
inline auto InvokeOnTypeIDsForCppTypes(ArgTypes &&...args) {
  constexpr std::size_t last = sizeof...(ArgTypes) - 1;
  const auto &functor = std::get<last>(std::forward_as_tuple(args...));

  return internal::InvokeOnTypeIDsImpl<0, meta::TypeList<>>(
      std::forward_as_tuple(args...),
      [&](auto typelist) {
    using CppTypeList =
        typename decltype(typelist)
            ::template map<internal::GetCppTypeForTypeID>;  // NOLINT[build/include_what_you_use]
    functor(CppTypeList());
  });
}

template <typename Functor>
inline auto InvokeOnTypeVectorForCppTypes(const std::vector<const Type*> &types,
                                          const Functor &functor) {
  if (types.empty()) {
    LOG(FATAL) << "Invalid number of types";
  }
  // TODO(robin-team): Temporary hack for compile timeout ...
  for (const auto &type : types) {
    CHECK(type->getTypeID() == kUInt32);
  }
  switch (types.size()) {
    case 1u:
      return functor(meta::TypeList<std::uint32_t>());
    case 2u:
      return functor(meta::TypeList<std::uint32_t, std::uint32_t>());
    case 3u:
      return functor(meta::TypeList<std::uint32_t, std::uint32_t, std::uint32_t>());
/*
    case 4u:
      return InvokeOnTypeIDsForCppTypes(
          types[0]->getTypeID(), types[1]->getTypeID(),
          types[2]->getTypeID(), types[3]->getTypeID(), functor);
*/
    default:
      break;
  }
  LOG(FATAL) << "Cannot not handle more than 3 types";
}

}  // namespace project

#endif  // PROJECT_TYPES_TYPE_HPP_
