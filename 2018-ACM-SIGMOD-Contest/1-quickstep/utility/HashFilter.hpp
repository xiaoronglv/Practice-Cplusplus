#ifndef PROJECT_UTILITY_HASH_FILTER_HPP_
#define PROJECT_UTILITY_HASH_FILTER_HPP_

#include <cstddef>
#include <cstring>
#include <memory>

#include "utility/ConcurrentBitVector.hpp"
#include "utility/BitVector.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

template <typename BitVectorImpl>
class HashFilterImpl {
 public:
  typedef BitVectorImpl BitVectorType;

  explicit HashFilterImpl(const std::size_t cardinality)
      : cardinality_(cardinality),
        bit_vector_(cardinality) {}

  std::size_t getCardinality() const {
    return cardinality_;
  }

  inline void insert(const std::size_t value) {
    bit_vector_.setBit(value % cardinality_);
  }

  inline bool contains(const std::size_t value) const {
    return bit_vector_.getBit(value % cardinality_);
  }

  const BitVectorImpl& getBitVector() const {
    return bit_vector_;
  }

  BitVectorImpl* getBitVectorMutable() {
    return &bit_vector_;
  }

  template <typename OtherHashFilter>
  auto* clone() const {
    static_assert(sizeof(typename BitVectorType::DataType) ==
                  sizeof(typename OtherHashFilter::BitVectorType::DataType),
                  "Invalid assumption on atomic layout");

    std::unique_ptr<OtherHashFilter> other =
        std::make_unique<OtherHashFilter>(cardinality_);

    std::memcpy(other->getBitVectorMutable()->getDataMutable(),
                bit_vector_.getData(),
                bit_vector_.getDataSize() * sizeof(typename BitVectorType::DataType));

    return other.release();
  }

  template <typename OtherHashFilter>
  void intersectWith(const OtherHashFilter &other) {
    static_assert(sizeof(typename BitVectorType::DataType) ==
                  sizeof(typename OtherHashFilter::BitVectorType::DataType),
                  "Invalid assumption on atomic layout");

    DCHECK_EQ(cardinality_, other.getCardinality());
    DCHECK_EQ(bit_vector_.getDataSize(), other.getBitVector().getDataSize());

    const auto *src = other.getBitVector().getData();
    auto *dst = bit_vector_.getDataMutable();

    for (std::size_t i = 0; i < bit_vector_.getDataSize(); ++i) {
      dst[i] &= src[i];
    }
  }

 private:
  const std::size_t cardinality_;
  BitVectorImpl bit_vector_;

  DISALLOW_COPY_AND_ASSIGN(HashFilterImpl);
};

using HashFilter = HashFilterImpl<BitVector>;
using ConcurrentHashFilter = HashFilterImpl<ConcurrentBitVector>;

}  // namespace project

#endif  // PROJECT_UTILITY_HASH_FILTER_HPP_
