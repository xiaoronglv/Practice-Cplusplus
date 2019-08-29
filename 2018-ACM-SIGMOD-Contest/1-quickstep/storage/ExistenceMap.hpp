#ifndef PROJECT_STORAGE_EXISTENCE_MAP_HPP_
#define PROJECT_STORAGE_EXISTENCE_MAP_HPP_

#include <cstddef>
#include <cstdint>
#include <iostream>

#include "utility/BitVector.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class ExistenceMap {
 public:
  ExistenceMap(const std::uint64_t min_value,
               const std::uint64_t max_value)
      : min_value_(RoundDown64(min_value)),
        max_value_(max_value),
        bit_vector_(max_value_ - min_value_ + 1) {
  }

  explicit ExistenceMap(const Range &domain)
      : ExistenceMap(domain.begin(), domain.end() - 1) {
    DCHECK_NE(0u, domain.end());
  }

  inline Range getRange() const {
    return Range(min_value_, max_value_ + 1);
  }

  inline bool getBit(const std::uint64_t value) const {
    DCHECK_GE(value, min_value_);
    DCHECK_LE(value, max_value_);
    return bit_vector_.getBit(value - min_value_);
  }

  inline void setBit(const std::uint64_t value) const {
    DCHECK_GE(value, min_value_);
    DCHECK_LE(value, max_value_);
    bit_vector_.setBit(value - min_value_);
  }

  inline std::size_t onesCount() const {
    return bit_vector_.onesCount();
  }

  bool isSubsetOf(const ExistenceMap &rhs) const {
    const ExistenceMap &lhs = *this;

    const std::size_t lhs_base = lhs.min_value_;
    const std::size_t rhs_base = rhs.min_value_;

    const std::size_t lhs_size = lhs.bit_vector_.getDataSize();
    const std::size_t rhs_size = rhs.bit_vector_.getDataSize();

    const std::size_t lhs_max = lhs_base + (lhs_size << kHigherOrderShift);
    const std::size_t rhs_max = rhs_base + (rhs_size << kHigherOrderShift);

    // No intersection.
    if (lhs_base >= rhs_max || lhs_max <= rhs_base) {
      return allZeros(0, lhs_size);
    }

    // Align left.
    std::size_t lhs_begin = 0, rhs_begin = 0;
    if (lhs_base >= rhs_base) {
      const std::size_t delta = lhs_base - rhs_base;
      DCHECK_EQ(0u, (delta & kLowerOrderMask));
      rhs_begin += delta >> kHigherOrderShift;
    } else {
      const std::size_t delta = rhs_base - lhs_base;
      DCHECK_EQ(0u, (delta & kLowerOrderMask));
      lhs_begin += delta >> kHigherOrderShift;

      if (!allZeros(0, lhs_begin)) {
        return false;
      }
    }

    // Align right.
    std::size_t lhs_end = lhs_size, rhs_end = rhs_size;
    if (lhs_max <= rhs_max) {
      const std::size_t delta = rhs_max - lhs_max;
      DCHECK_EQ(0u, (delta & kLowerOrderMask));
      rhs_end -= delta >> kHigherOrderShift;
    } else {
      const std::size_t delta = lhs_max - rhs_max;
      DCHECK_EQ(0u, (delta & kLowerOrderMask));
      lhs_end -= delta >> kHigherOrderShift;

      if (!allZeros(lhs_end, lhs_size)) {
        return false;
      }
    }

    DCHECK_LE(lhs_begin, lhs_end);
    DCHECK_LE(rhs_begin, rhs_end);
    DCHECK_EQ(lhs_end - lhs_begin, rhs_end - rhs_begin);

    const std::size_t *lhs_data = lhs.bit_vector_.getData() + lhs_begin;
    const std::size_t *rhs_data = rhs.bit_vector_.getData() + rhs_begin;

    const std::size_t range = lhs_end - lhs_begin;
    for (std::size_t i = 0; i < range; ++i) {
      const std::size_t lhs_value = lhs_data[i];
      const std::size_t rhs_value = rhs_data[i];
      if ((lhs_value & rhs_value) != lhs_value) {
        return false;
      }
    }
    return true;
  }

  void unionWith(const ExistenceMap &other) {
    auto &lhs = bit_vector_;
    const auto &rhs = other.bit_vector_;

    const std::size_t length = lhs.getDataSize();
    DCHECK_EQ(length, rhs.getDataSize());

    std::size_t *lhs_data = lhs.getDataMutable();
    const std::size_t *rhs_data = rhs.getData();

    for (std::size_t i = 0; i < length; ++i) {
      lhs_data[i] |= rhs_data[i];
    }
  }

  static std::uint64_t RoundDown64(const std::uint64_t range) {
    return (range >> kHigherOrderShift) << kHigherOrderShift;
  }

 private:
  inline bool allZeros(const std::size_t begin, const std::size_t end) const {
    const std::size_t *data = bit_vector_.getData();
    for (std::size_t i = begin; i < end; ++i) {
      if (data[i] != 0) {
        return false;
      }
    }
    return true;
  }

  static constexpr std::size_t kLowerOrderMask = 0x3f;
  static constexpr std::size_t kHigherOrderShift = 6;

  const std::uint64_t min_value_;
  const std::uint64_t max_value_;

  BitVector bit_vector_;

  DISALLOW_COPY_AND_ASSIGN(ExistenceMap);
};

}  // namespace project

#endif  // PROJECT_STORAGE_EXISTENCE_MAP_HPP_
