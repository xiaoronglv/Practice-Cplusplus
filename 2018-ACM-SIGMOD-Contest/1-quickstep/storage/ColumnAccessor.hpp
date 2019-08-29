#ifndef PROJECT_STORAGE_COLUMN_ACCESSOR_HPP_
#define PROJECT_STORAGE_COLUMN_ACCESSOR_HPP_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <type_traits>

#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class ColumnAccessor {
 public:
  enum ColumnAccessorType {
    kColumnStoreAccessor = 0,
    kRowStoreAccessor
  };

  ColumnAccessor() {}

  virtual ~ColumnAccessor() {}

  virtual ColumnAccessorType getColumnAccessorType() const = 0;

  virtual std::size_t getTypeSize() const = 0;

  virtual std::size_t getNumTuples() const = 0;

  virtual ColumnAccessor* createSubset(const Range &range) const = 0;

  template <typename T>
  static T* Cast(ColumnAccessor *accessor) {
    DCHECK(accessor->getColumnAccessorType() == T::kStaticType);
    return static_cast<T*>(accessor);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnAccessor);
};

template <typename T>
class ColumnStoreColumnAccessor : public ColumnAccessor {
 public:
  typedef T ValueType;
  static constexpr ColumnAccessorType kStaticType = ColumnAccessorType::kColumnStoreAccessor;

  ColumnStoreColumnAccessor(const std::size_t length, const T *data)
      : length_(length), data_(data) {}

  ColumnAccessorType getColumnAccessorType() const override {
    return kColumnStoreAccessor;
  }

  std::size_t getTypeSize() const override {
    return sizeof(T);
  }

  std::size_t getNumTuples() const override {
    return length_;
  }

  inline const T* getData() const {
    return data_;
  }

  inline const T& at(const std::size_t pos) const {
    return data_[pos];
  }

  inline const T& operator[](const std::size_t pos) const {
    return data_[pos];
  }

  template <typename Functor>
  inline void forEach(const Functor &functor) const {
    for (std::size_t i = 0; i < length_; ++i) {
      functor(data_[i]);
    }
  }

  ColumnAccessor* createSubset(const Range &range) const override {
    return new ColumnStoreColumnAccessor<T>(range.size(), data_ + range.begin());
  }

 private:
  const std::size_t length_;
  const T *data_;

  DISALLOW_COPY_AND_ASSIGN(ColumnStoreColumnAccessor);
};

template <typename T>
class RowStoreColumnAccessor : public ColumnAccessor {
 public:
  typedef T ValueType;
  static constexpr ColumnAccessorType kStaticType = ColumnAccessorType::kRowStoreAccessor;

  RowStoreColumnAccessor(const std::size_t num_rows,
                         const std::size_t stride,
                         const void *data)
      : num_rows_(num_rows), stride_(stride), data_(data) {}

  ColumnAccessorType getColumnAccessorType() const override {
    return kRowStoreAccessor;
  }

  std::size_t getTypeSize() const override {
    return sizeof(T);
  }

  std::size_t getNumTuples() const override {
    return num_rows_;
  }

  inline const void* getData() const {
    return data_;
  }

  inline const T& at(const std::size_t pos) const {
    const void *row = static_cast<const char*>(data_) + stride_ * pos;
    return *static_cast<const T*>(row);
  }

  inline const T& operator[](const std::size_t pos) const {
    return at(pos);
  }

  ColumnAccessor* createSubset(const Range &range) const override {
    const void *row_begin =
        static_cast<const char*>(data_) + stride_ * range.begin();
    return new RowStoreColumnAccessor<T>(range.size(), stride_, row_begin);
  }

 private:
  const std::size_t num_rows_;
  const std::size_t stride_;
  const void *data_;

  DISALLOW_COPY_AND_ASSIGN(RowStoreColumnAccessor);
};


template <typename Functor>
auto InvokeOnColumnAccessor(const ColumnAccessor *accessor,
                            const Functor &functor) {
  switch (accessor->getColumnAccessorType()) {
    case ColumnAccessor::kColumnStoreAccessor:
      switch (accessor->getTypeSize()) {
        case sizeof(std::uint32_t):
          return functor(
              static_cast<const ColumnStoreColumnAccessor<std::uint32_t>*>(accessor));
        case sizeof(std::uint64_t):
          return functor(
              static_cast<const ColumnStoreColumnAccessor<std::uint64_t>*>(accessor));
        default:
          break;
      }
      break;
    case ColumnAccessor::kRowStoreAccessor:
      // We may consider using RowStoreBlock as temporary stores.
      break;
    default:
      break;
  }
  LOG(FATAL) << "Unsupported ColumnAccessor type";
}

template <typename Functor>
auto InvokeOnColumnAccessorMakeShared(ColumnAccessor *accessor,
                                      const Functor &functor) {
  return InvokeOnColumnAccessor(
      accessor,
      [&functor](auto *accessor) {
    using Accessor = std::remove_pointer_t<decltype(accessor)>;
    return functor(std::shared_ptr<Accessor>(static_cast<Accessor*>(accessor)));
  });
}

}  // namespace project

#endif  // PROJECT_STORAGE_COLUMN_ACCESSOR_HPP_
