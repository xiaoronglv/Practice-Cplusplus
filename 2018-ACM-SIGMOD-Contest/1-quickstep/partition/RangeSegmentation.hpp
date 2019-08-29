#ifndef PROJECT_PARTITION_RANGE_SEGMENTATION_HPP_
#define PROJECT_PARTITION_RANGE_SEGMENTATION_HPP_

#include <cstddef>

#include "utility/BitManipulation.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class RangeSegmentation {
 public:
  RangeSegmentation(const Range &range,
                    const std::size_t segment_width)
      : range_(range),
        segment_shift_(CalculateShift(segment_width)),
        num_segments_(CalculateNumSegments(range.size(), segment_shift_)) {}

  RangeSegmentation(const RangeSegmentation &other)
      : range_(other.range_),
        segment_shift_(other.segment_shift_),
        num_segments_(other.num_segments_) {}

  inline std::size_t getNumPartitions() const {
    return num_segments_;
  }

  inline std::size_t operator()(const std::size_t value) const {
    DCHECK_GE(value, range_.begin());
    DCHECK_LT(value, range_.end());
    return (value - range_.begin()) >> segment_shift_;
  }

 private:
  inline static std::size_t CalculateShift(const std::size_t segment_width) {
    CHECK_EQ(1u, PopulationCount(segment_width));
    return TrailingZeroCount(segment_width);
  }

  inline static std::size_t CalculateNumSegments(const std::size_t length,
                                                 const std::size_t segment_shift) {
    return (length >> segment_shift) +
           ((length & ((1 << segment_shift) - 1)) ? 1 : 0);
  }

  const Range range_;
  const std::size_t segment_shift_;
  const std::size_t num_segments_;
};

}  // namespace project

#endif  // PROJECT_PARTITION_RANGE_SEGMENTATION_HPP_
