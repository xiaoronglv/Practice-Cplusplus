#ifndef PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_HPP_
#define PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

#include "scheduler/Task.hpp"
#include "storage/ExistenceMap.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/meta/TypeList.hpp"

#include "glog/logging.h"

namespace project {

class KeyCountVector {
 public:
  enum CountVectorType {
    kArray = 0,
    kBitVector
  };

  KeyCountVector() {}

  virtual ~KeyCountVector() {}

  virtual CountVectorType getCountVectorType() const = 0;

  virtual std::size_t getTypeSize() const = 0;

  virtual bool isAtomic() const = 0;

  virtual Range getRange() const = 0;

  virtual KeyCountVector* createSubset(const Range &range) const = 0;

  template <typename Functor>
  void forEachPair(const Functor &functor) const;

 private:
  DISALLOW_COPY_AND_ASSIGN(KeyCountVector);
};

template <bool atomic>
struct KeyCountVectorTrait;

template <>
struct KeyCountVectorTrait<true> {
  template <typename DataType>
  inline static void Increase(DataType *data, const std::size_t pos) {
    data[pos].fetch_add(1, std::memory_order_relaxed);
  }
  template <typename DataType, typename T>
  inline static void Increase(DataType *data,
                              const std::size_t pos,
                              const T &value) {
    data[pos].fetch_add(value, std::memory_order_relaxed);
  }
  template <typename DataType>
  inline static std::uint32_t Get(DataType *data, const std::size_t pos) {
    return data[pos].load(std::memory_order_relaxed);
  }
};

template <>
struct KeyCountVectorTrait<false> {
  template <typename DataType>
  inline static void Increase(DataType *data, const std::size_t pos) {
    ++data[pos];
  }
  template <typename DataType, typename T>
  inline static void Increase(DataType *data,
                              const std::size_t pos,
                              const T &value) {
    data[pos] += value;
  }
  template <typename DataType>
  inline static std::uint32_t Get(DataType *data, const std::size_t pos) {
    return data[pos];
  }
};

template <typename T, bool atomic = false>
class KeyCountVectorImpl : public KeyCountVector {
 public:
  typedef std::size_t ValueType;
  typedef KeyCountVectorImpl<T, atomic> KeyCountVectorType;
  typedef std::conditional_t<atomic, std::atomic<T>, T> DataType;

  using Trait = KeyCountVectorTrait<atomic>;

  KeyCountVectorImpl(const std::size_t base,
                     const std::size_t length,
                     const bool initialize = true)
      : base_(base),
        length_(length),
        data_(static_cast<DataType*>(std::malloc(length * sizeof(DataType)))),
        owns_(true) {
    if (initialize) {
      std::memset(data_, 0, length * sizeof(DataType));
    }
  }

  KeyCountVectorImpl(const Range &range,
                     const bool initialize = true)
      : KeyCountVectorImpl(range.begin(), range.size(), initialize) {}

  KeyCountVectorImpl(const std::size_t base,
                     const std::size_t length,
                     DataType *data,
                     const bool owns)
      : base_(base),
        length_(length),
        data_(data),
        owns_(owns) {}

  ~KeyCountVectorImpl() {
    if (owns_) {
      std::free(data_);
    }
  }

  void initialize(Task *ctx) {
    constexpr std::size_t kBatchSize = 1000000;
    const RangeSplitter splitter =
        RangeSplitter::CreateWithPartitionLength(0, length_, kBatchSize);
    for (const Range range : splitter) {
      ctx->spawnLambdaTask([this, range] {
        std::memset(this->data_ + range.begin(),
                    0,
                    range.size() * sizeof(DataType));
      });
    }
  }

  CountVectorType getCountVectorType() const override {
    return kArray;
  }

  std::size_t getTypeSize() const override {
    return sizeof(T);
  }

  bool isAtomic() const override {
    return atomic;
  }

  Range getRange() const override {
    return Range(begin(), end());
  }

  KeyCountVector* createSubset(const Range &range) const override {
    const Range overlap = Range(begin(), end()).intersect(range);
    DCHECK_LE(begin(), overlap.begin());
    DCHECK_GE(end(), overlap.end());

    return new KeyCountVectorType(overlap.begin(),
                                  overlap.size(),
                                  data_ + overlap.begin() - base_,
                                  false);
  }

  inline std::size_t length() const {
    return length_;
  }

  inline std::size_t begin() const {
    return base_;
  }

  inline std::size_t end() const {
    return base_ + length_;
  }

  inline std::uint32_t at(const std::size_t value) const {
    DCHECK_LE(begin(), value);
    DCHECK_GT(end(), value);
    return Trait::Get(data_, value - base_);
  }

  inline std::uint32_t atChecked(const std::size_t value) const {
    if (value < begin() || value >= end()) {
      return 0;
    }
    return Trait::Get(data_, value - base_);
  }

  template <bool check>
  inline std::uint32_t atConditional(const std::size_t value,
                                     std::enable_if_t<check> * = 0) const {
    return atChecked(value);
  }

  template <bool check>
  inline std::uint32_t atConditional(const std::size_t value,
                                     std::enable_if_t<!check> * = 0) const {
    return at(value);
  }

  inline void increaseCount(const std::size_t value) {
    DCHECK_LE(begin(), value);
    DCHECK_GT(end(), value);
    Trait::Increase(data_, value - base_);
  }

  inline void increaseCount(const std::size_t value, const std::size_t count) {
    DCHECK_LE(begin(), value);
    DCHECK_GT(end(), value);
    Trait::Increase(data_, value - base_, count);
  }

  template <typename OtherKeyCountVector>
  void mergeWith(const OtherKeyCountVector &other) {
    DCHECK_EQ(base_, other.base_);
    DCHECK_EQ(length_, other.length_);

    using OtherTrait = typename OtherKeyCountVector::Trait;
    for (std::size_t i = 0; i < length_; ++i) {
      Trait::Increase(data_, i, OtherTrait::Get(other.data_, i));
    }
  }

  template <typename Functor>
  void forEachPair(const Functor &functor) const {
    for (std::size_t i = 0; i < length_; ++i) {
      const std::uint32_t count = Trait::Get(data_, i);
      if (count != 0) {
        functor(base_ + i, count);
      }
    }
  }

 private:
  const std::size_t base_;
  const std::size_t length_;
  DataType *data_;
  bool owns_;

  DISALLOW_COPY_AND_ASSIGN(KeyCountVectorImpl);
};

class BitCountVector : public KeyCountVector {
 public:
  BitCountVector(const Range &domain,
                 const ExistenceMap &existence_map)
      : base_(domain.begin()),
        length_(domain.size()),
        existence_map_(existence_map) {
    DCHECK(existence_map_.getRange().contains(domain));
  }

  CountVectorType getCountVectorType() const override {
    return kBitVector;
  }

  std::size_t getTypeSize() const override {
    LOG(FATAL) << "Unsupported call to BitCountVector::getTypeSize()";
  }

  bool isAtomic() const override {
    return false;
  }

  Range getRange() const override {
    return Range(begin(), end());
  }

  KeyCountVector* createSubset(const Range &range) const override {
    const Range overlap = Range(begin(), end()).intersect(range);
    DCHECK_LE(begin(), overlap.begin());
    DCHECK_GE(end(), overlap.end());

    return new BitCountVector(overlap, existence_map_);
  }

  inline std::size_t length() const {
    return length_;
  }

  inline std::size_t begin() const {
    return base_;
  }

  inline std::size_t end() const {
    return base_ + length_;
  }

  inline std::uint32_t at(const std::size_t value) const {
    DCHECK_LE(begin(), value);
    DCHECK_GT(end(), value);
    return existence_map_.getBit(value);
  }

  inline std::uint32_t atChecked(const std::size_t value) const {
    if (value < begin() || value >= end()) {
      return 0;
    }
    return existence_map_.getBit(value);
  }

  template <bool check>
  inline std::uint32_t atConditional(const std::size_t value,
                                     std::enable_if_t<check> * = 0) const {
    return atChecked(value);
  }

  template <bool check>
  inline std::uint32_t atConditional(const std::size_t value,
                                     std::enable_if_t<!check> * = 0) const {
    return at(value);
  }

  template <typename Functor>
  void forEachPair(const Functor &functor) const {
    for (std::size_t value = begin(); value < end(); ++value) {
      if (existence_map_.getBit(value)) {
        functor(value, 1u);
      }
    }
  }

 private:
  const std::size_t base_;
  const std::size_t length_;
  const ExistenceMap &existence_map_;

  DISALLOW_COPY_AND_ASSIGN(BitCountVector);
};

// ----------------------------------------------------------------------------
// Utility functions.

template <bool atomic = false, typename Functor>
inline auto InvokeOnKeyCountVector(const KeyCountVector &kcv,
                                   const Functor &functor) {
  switch (kcv.getCountVectorType()) {
    case KeyCountVector::kArray:
      switch (kcv.getTypeSize()) {
        case 1u:
          return functor(
              static_cast<const KeyCountVectorImpl<std::uint8_t, atomic>&>(kcv));
        case 4u:
          return functor(
              static_cast<const KeyCountVectorImpl<std::uint32_t, atomic>&>(kcv));
        default:
          break;
      }
      break;
    case KeyCountVector::kBitVector:
      return functor(static_cast<const BitCountVector&>(kcv));
    default:
      break;
  }
  LOG(FATAL) << "Unsupported count vector type";
}

template <typename Functor>
inline auto InvokeOnMaxCount(const std::size_t max_count,
                             const Functor &functor) {
  if (max_count <= std::numeric_limits<std::uint8_t>::max()) {
    return functor(meta::TypeList<std::uint8_t>());
  } else {
    CHECK_LE(max_count, std::numeric_limits<std::uint32_t>::max());
    return functor(meta::TypeList<std::uint32_t>());
  }
}

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Functor>
void KeyCountVector::forEachPair(const Functor &functor) const {
  InvokeOnKeyCountVector(
      *this, [&](const auto &cv) {
    cv.forEachPair(functor);
  });
}

}  // namespace project

#endif  // PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_HPP_
