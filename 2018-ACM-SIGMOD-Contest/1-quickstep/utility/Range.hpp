#ifndef PROJECT_UTILITY_RANGE_HPP_
#define PROJECT_UTILITY_RANGE_HPP_

#include <algorithm>
#include <cstddef>
#include <limits>
#include <string>

#include "utility/NumericGenerator.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class Range {
 public:
  Range() : begin_(0), end_(0) {}

  Range(const std::size_t begin, const std::size_t end)
      : begin_(begin), end_(end) {
    DCHECK_LE(begin_, end_);
  }

  Range(const Range &other)
      : begin_(other.begin_), end_(other.end_) {
    DCHECK_LE(begin_, end_);
  }

  Range& operator=(const Range &other) {
    begin_ = other.begin_;
    end_ = other.end_;
    DCHECK_LE(begin_, end_);
    return *this;
  }

  inline std::size_t begin() const {
    return begin_;
  }

  inline std::size_t end() const {
    return end_;
  }

  inline std::size_t size() const {
    return end_ - begin_;
  }

  inline bool valid() const {
    return end_ != std::numeric_limits<std::size_t>::max();
  }

  inline bool contains(const std::size_t value) const {
    return begin_ <= value && end_ > value;
  }

  inline bool contains(const Range &other) const {
    return begin_ <= other.begin_ && end_ >= other.end_;
  }

  inline NumericGenerator<std::size_t, 1u> getGenerator() const {
    return NumericGenerator<std::size_t, 1u>(begin_, end_);
  }

  inline bool operator==(const Range &other) const {
    return begin_ == other.begin_ && end_ == other.end_;
  }

  inline Range intersect(const Range &other) const {
    if (other.begin_ >= end_ || other.end_ <= begin_) {
      return Range(begin_, begin_);
    }

    return Range(std::max(begin_, other.begin_),
                 std::min(end_, other.end_));
  }


  inline static Range Max() {
    return Range(0, std::numeric_limits<std::size_t>::max());
  }

  std::string toString() const {
    return "[" + std::to_string(begin_) + ", " + std::to_string(end_) + ")";
  }

 private:
  std::size_t begin_;
  std::size_t end_;
};

class RangeSplitter {
 public:
  class ConstIterator {
   public:
    ConstIterator()
        : splitter_(nullptr) {}

    ConstIterator(const RangeSplitter *splitter,
                  const std::size_t current_position)
        : splitter_(splitter),
          current_position_(current_position) {}

    inline Range operator*() const {
      DCHECK(splitter_ != nullptr);
      DCHECK(current_position_ < splitter_->size());
      return splitter_->at(current_position_);
    }

    inline ConstIterator& operator++() {
      DCHECK(splitter_ != nullptr);
      ++current_position_;
      return *this;
    }

    inline ConstIterator& operator--() {
      DCHECK(splitter_ != nullptr);
      --current_position_;
      return *this;
    }

    inline ConstIterator operator++(int) {
      ConstIterator result(*this);
      ++(*this);
      return result;
    }

    inline ConstIterator operator--(int) {
      ConstIterator result(*this);
      --(*this);
      return result;
    }

    inline bool operator==(const ConstIterator& other) const {
      return splitter_ == other.splitter_
          && current_position_ == other.current_position_;
    }

    inline bool operator!=(const ConstIterator& other) const {
      return !(*this == other);
    }

   private:
    const RangeSplitter *splitter_;
    std::size_t current_position_;
  };

  using const_iterator = ConstIterator;

  RangeSplitter(const RangeSplitter &other)
      : begin_(other.begin_), end_(other.end_),
        num_partitions_(other.num_partitions_),
        partition_length_(other.partition_length_) {}


  inline std::size_t size() const {
    return num_partitions_;
  }

  inline Range operator[](const std::size_t partition_id) const {
    return at(partition_id);
  }

  inline Range at(const std::size_t partition_id) const {
    DCHECK_LT(partition_id, num_partitions_);
    const std::size_t begin = begin_ + partition_length_ * partition_id;
    const std::size_t end =
        partition_id == num_partitions_ - 1
            ? end_
            : begin + partition_length_;
    return Range(begin, end);
  }

  inline const_iterator begin() const {
    return const_iterator(this, 0);
  }

  inline const_iterator end() const {
    return const_iterator(this, size());
  }

 private:
  RangeSplitter(const std::size_t begin,
                const std::size_t end,
                const std::size_t num_partitions,
                const std::size_t partition_length)
      : begin_(begin),
        end_(end),
        num_partitions_(num_partitions),
        partition_length_(partition_length) {
    DCHECK_LE(num_partitions_ * partition_length_, end_);
  }

  static constexpr std::size_t kMaxNumPartitions =
      std::numeric_limits<std::size_t>::max();

  const std::size_t begin_;
  const std::size_t end_;
  const std::size_t num_partitions_;
  const std::size_t partition_length_;

 public:
  static RangeSplitter CreateWithPartitionLength(
      const std::size_t begin,
      const std::size_t end,
      const std::size_t min_partition_length,
      const std::size_t max_num_partitions = kMaxNumPartitions) {
    DCHECK_LE(begin, end);
    DCHECK_GT(min_partition_length, 0u);
    DCHECK_GT(max_num_partitions, 0u);

    const std::size_t range_length = end - begin;
    const std::size_t est_num_partitions = range_length / min_partition_length;

    const std::size_t num_partitions =
        std::max(1uL, std::min(est_num_partitions, max_num_partitions));
    const std::size_t partition_length = range_length / num_partitions;
    return RangeSplitter(begin, end, num_partitions, partition_length);
  }

  static RangeSplitter CreateWithPartitionLength(
      const Range &range,
      const std::size_t min_partition_length,
      const std::size_t max_num_partitions = kMaxNumPartitions) {
    return CreateWithPartitionLength(
        range.begin(), range.end(), min_partition_length, max_num_partitions);
  }

  static RangeSplitter CreateWithMinMaxPartitionLength(
      const std::size_t begin,
      const std::size_t end,
      const std::size_t min_partition_length,
      const std::size_t max_partition_length,
      const std::size_t ept_num_partitions) {
    DCHECK_LE(begin, end);
    DCHECK_LE(min_partition_length, max_partition_length);
    DCHECK_GT(min_partition_length, 0u);
    DCHECK_GT(max_partition_length, 0u);

    const std::size_t range_length = end - begin;
    const std::size_t ept_partition_length = range_length / ept_num_partitions;

    std::size_t partition_length;
    if (ept_partition_length < min_partition_length) {
      partition_length = min_partition_length;
    } else if (ept_partition_length > max_partition_length) {
      partition_length = max_partition_length;
    } else {
      partition_length = ept_partition_length;
    }

    const std::size_t num_partitions =
        std::max(1uL, range_length / partition_length);
    return RangeSplitter(begin, end, num_partitions, partition_length);
  }

  static RangeSplitter CreateWithMinMaxPartitionLength(
      const Range &range,
      const std::size_t min_partition_length,
      const std::size_t max_partition_length,
      const std::size_t ept_num_partitions) {
    return CreateWithMinMaxPartitionLength(
        range.begin(), range.end(),
        min_partition_length, max_partition_length,
        ept_num_partitions);
  }

  static RangeSplitter CreateWithNumPartitions(
      const std::size_t begin,
      const std::size_t end,
      const std::size_t num_partitions) {
    DCHECK_LE(begin, end);
    DCHECK_GT(num_partitions, 0u);

    const std::size_t partition_length = (end - begin) / num_partitions;
    return RangeSplitter(begin, end, num_partitions, partition_length);
  }

  static RangeSplitter CreateWithNumPartitions(
      const Range &range,
      const std::size_t num_partitions) {
    return CreateWithNumPartitions(range.begin(), range.end(), num_partitions);
  }
};

}  // namespace project

#endif  // PROJECT_UTILITY_RANGE_HPP_
