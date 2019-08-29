#ifndef PROJECT_STORAGE_ATTRIBUTE_STATISTICS_HPP_
#define PROJECT_STORAGE_ATTRIBUTE_STATISTICS_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

enum class SortOrder {
  kAscendant = 0,
  kDescendant,
  kNotSorted,
  kUnknown
};

inline std::string GetSortOrderString(const SortOrder sort_order) {
  switch (sort_order) {
    case SortOrder::kAscendant:
      return "Ascendant";
    case SortOrder::kDescendant:
      return "Descendant";
    case SortOrder::kNotSorted:
      return "NotSorted";
    default:
      break;
  }
  return "Unknown";
}

class AttributeStatistics {
 public:
  AttributeStatistics() : sort_order_(SortOrder::kUnknown) {}

  SortOrder getSortOrder() const {
    return sort_order_;
  }

  void setSortOrder(const SortOrder sort_order) {
    sort_order_ = sort_order;
  }

  std::string getSortOrderString() const {
    return GetSortOrderString(sort_order_);
  }

  bool hasMinValue() const {
    return min_value_ != nullptr;
  }

  std::uint64_t getMinValue() const {
    DCHECK(hasMinValue());
    return *min_value_;
  }

  void setMinValue(const std::uint64_t value) {
    min_value_ = std::make_unique<std::uint64_t>(value);
  }

  bool hasMaxValue() const {
    return max_value_ != nullptr;
  }

  std::uint64_t getMaxValue() const {
    DCHECK(hasMaxValue());
    return *max_value_;
  }

  void setMaxValue(const std::uint64_t value) {
    max_value_ = std::make_unique<std::uint64_t>(value);
  }

  bool hasNumDistinctValues() const {
    return num_distinct_values_ != nullptr;
  }

  std::size_t getNumDistinctValues() const {
    DCHECK(hasNumDistinctValues());
    return *num_distinct_values_;
  }

  void setNumDistinctValues(const std::size_t value) {
    num_distinct_values_ = std::make_unique<std::size_t>(value);
  }

 private:
  std::unique_ptr<std::uint64_t> min_value_;
  std::unique_ptr<std::uint64_t> max_value_;
  std::unique_ptr<std::size_t> num_distinct_values_;
  SortOrder sort_order_;

  DISALLOW_COPY_AND_ASSIGN(AttributeStatistics);
};

}  // namespace project

#endif  // PROJECT_STORAGE_ATTRIBUTE_STATISTICS_HPP_
