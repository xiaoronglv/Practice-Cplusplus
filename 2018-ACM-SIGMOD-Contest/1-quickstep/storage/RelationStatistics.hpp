#ifndef PROJECT_STORAGE_RELATION_STATISTICS_HPP_
#define PROJECT_STORAGE_RELATION_STATISTICS_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "storage/AttributeStatistics.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class RelationStatistics {
 public:
  explicit RelationStatistics(const std::size_t num_attributes)
      : num_attributes_(num_attributes),
        num_tuples_(0),
        attr_stats_(num_attributes) {}

  std::uint64_t getNumTuples() const {
    return num_tuples_.load(std::memory_order_relaxed);
  }

  void increaseNumTuples(const std::uint64_t delta) {
    num_tuples_.fetch_add(delta, std::memory_order_relaxed);
  }

  const AttributeStatistics& getAttributeStatistics(const attribute_id id) const {
    DCHECK_LT(id, num_attributes_);
    return attr_stats_[id];
  }

  AttributeStatistics& getAttributeStatisticsMutable(const attribute_id id) {
    DCHECK_LT(id, num_attributes_);
    return attr_stats_[id];
  }

 private:
  const relation_id num_attributes_;
  std::atomic<std::uint64_t> num_tuples_;
  std::vector<AttributeStatistics> attr_stats_;

  DISALLOW_COPY_AND_ASSIGN(RelationStatistics);
};

}  // namespace project

#endif  // PROJECT_STORAGE_RELATION_STATISTICS_HPP_
