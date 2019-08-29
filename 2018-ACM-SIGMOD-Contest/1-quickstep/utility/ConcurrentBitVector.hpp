#ifndef PROJECT_UTILITY_CONCURRENT_BIT_VECTOR_HPP_
#define PROJECT_UTILITY_CONCURRENT_BIT_VECTOR_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>

#include "utility/BitManipulation.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class ConcurrentBitVector {
 public:
  typedef std::atomic<std::size_t> DataType;

  ConcurrentBitVector(void *memory_location,
                      const std::size_t num_bits,
                      const bool initialize)
      : owned_(false),
        num_bits_(num_bits),
        data_array_(static_cast<DataType *>(memory_location)),
        data_array_size_((num_bits >> kHigherOrderShift) +
                         (num_bits & kLowerOrderMask ? 1 : 0)) {
    DCHECK_GT(num_bits, 0u);
    DCHECK(data_array_ != nullptr);

    if (initialize) {
      clear();
    }
  }

  explicit ConcurrentBitVector(const std::size_t num_bits,
                               const bool initialize = true)
      : owned_(true),
        num_bits_(num_bits),
        data_array_(static_cast<DataType *>(std::malloc(BytesNeeded(num_bits)))),
        data_array_size_((num_bits >> kHigherOrderShift) +
                         (num_bits & kLowerOrderMask ? 1 : 0)) {
    DCHECK_GT(num_bits, 0u);
    if (initialize) {
      clear();
    }
  }

  ~ConcurrentBitVector() {
    if (owned_) {
      std::free(data_array_);
    }
  }

  inline static std::size_t BytesNeeded(const std::size_t num_bits) {
    if (num_bits & kLowerOrderMask) {
      return ((num_bits >> kHigherOrderShift) + 1) * kDataSize;
    } else {
      return (num_bits >> kHigherOrderShift) * kDataSize;
    }
  }

  inline std::size_t getNumBytes() const {
    return BytesNeeded(num_bits_);
  }

  inline const DataType* getData() const {
    return data_array_;
  }

  inline DataType* getDataMutable() {
    return data_array_;
  }

  inline std::size_t getDataSize() const {
    return data_array_size_;
  }

  inline std::size_t size() const {
    return num_bits_;
  }

  inline void clear() {
    std::memset(data_array_, 0, BytesNeeded(num_bits_));
  }

  inline bool getBit(const std::size_t bit_num) const {
    const std::size_t data_value =
        data_array_[bit_num >> kHigherOrderShift].load(std::memory_order_relaxed);
    return (data_value << (bit_num & kLowerOrderMask)) & kTopBit;
  }

  inline void setBit(const std::size_t bit_num) const {
    data_array_[bit_num >> kHigherOrderShift].fetch_or(
        kTopBit >> (bit_num & kLowerOrderMask), std::memory_order_relaxed);
  }

  inline std::size_t firstOne(std::size_t position = 0) const {
    DCHECK_LT(position, num_bits_);

    const std::size_t position_index = position >> kHigherOrderShift;
    const std::size_t data_value =
        data_array_[position_index].load(std::memory_order_relaxed)
            & (std::numeric_limits<std::size_t>::max() >> (position & kLowerOrderMask));
    if (data_value) {
      return (position & ~kLowerOrderMask) | LeadingZeroCount<std::size_t>(data_value);
    }

    for (std::size_t array_idx = position_index + 1;
         array_idx < data_array_size_;
         ++array_idx) {
      const std::size_t data_value =
          data_array_[array_idx].load(std::memory_order_relaxed);
      if (data_value) {
        return (array_idx << kHigherOrderShift) | LeadingZeroCount<std::size_t>(data_value);
      }
    }

    return num_bits_;
  }

  inline std::size_t nextOne(const std::size_t position) const {
    const std::size_t search_pos = position + 1;
    return search_pos >= num_bits_ ? num_bits_ : firstOne(search_pos);
  }

  inline std::size_t onesCount() const {
    std::size_t count = 0;
    for (std::size_t array_idx = 0;
         array_idx < data_array_size_;
         ++array_idx) {
      count += PopulationCount<std::size_t>(
          data_array_[array_idx].load(std::memory_order_relaxed));
    }
    return count;
  }

  /**
   * @brief Count the total number of 1-bits in this bit vector within the
   *        specified range (start point INCLUSIVE but end point EXCLUSIVE).
   *
   * @param The start position of the range.
   * @param The end position of the range.
   *
   * @return The number of ones within the range, INCLUDING the 1-bit at
   *         \p start_position, but EXCLUDING the 1-bit at \p end_position.
   **/
  inline std::size_t onesCountInRange(const std::size_t start_position,
                                      const std::size_t end_position) const {
    DCHECK_LE(start_position, end_position);
    DCHECK_LT(start_position, num_bits_);
    DCHECK_LE(end_position, num_bits_);

    const std::size_t start_index = start_position >> kHigherOrderShift;
    const std::size_t end_index = end_position >> kHigherOrderShift;
    if (start_index == end_index) {
      const std::size_t data_value =
          data_array_[start_index].load(std::memory_order_relaxed)
              & (std::numeric_limits<std::size_t>::max() >> (start_position & kLowerOrderMask))
              &  ~(std::numeric_limits<std::size_t>::max() >> (end_position & kLowerOrderMask));
      return PopulationCount<std::size_t>(data_value);
    } else {
      const std::size_t first_data =
          data_array_[start_index].load(std::memory_order_relaxed)
              & (std::numeric_limits<std::size_t>::max() >> (start_position & kLowerOrderMask));
      std::size_t count = PopulationCount<std::size_t>(first_data);

      for (std::size_t array_idx = start_index + 1;
           array_idx < end_index;
           ++array_idx) {
        count += PopulationCount<std::size_t>(
            data_array_[array_idx].load(std::memory_order_relaxed));
      }

      const std::size_t last_offset = end_position & kLowerOrderMask;
      if (last_offset != 0) {
        const std::size_t last_data =
            data_array_[end_index].load(std::memory_order_relaxed)
                &  ~(std::numeric_limits<std::size_t>::max() >> last_offset);
        count += PopulationCount<std::size_t>(last_data);
      }

      return count;
    }
  }

 private:
  static constexpr std::size_t kDataSize = sizeof(DataType);

  // This works as long as the bit-width of size_t is power of 2:
  static constexpr std::size_t kLowerOrderMask = (sizeof(std::size_t) << 3) - 1;
  // This works for 32-bit or 64-bit size_t:
  static constexpr std::size_t kHigherOrderShift = sizeof(std::size_t) == 4 ? 5 : 6;

  static constexpr std::size_t kOne = static_cast<std::size_t>(1);
  static constexpr std::size_t kTopBit = kOne << kLowerOrderMask;

  const bool owned_;
  const std::size_t num_bits_;
  DataType *data_array_;
  const std::size_t data_array_size_;

  DISALLOW_COPY_AND_ASSIGN(ConcurrentBitVector);
};

}  // namespace project

#endif  // PROJECT_UTILITY_CONCURRENT_BIT_VECTOR_HPP_
