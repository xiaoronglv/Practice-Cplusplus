#ifndef PROJECT_PARTITION_PARTITION_DESTINATION_HPP_
#define PROJECT_PARTITION_PARTITION_DESTINATION_HPP_

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include "partition/PartitionDestinationBase.hpp"
#include "partition/PartitionGroup.hpp"
#include "utility/Macros.hpp"

namespace project {

template <typename Tuple>
class PartitionDestination : public PartitionDestinationBase {
 public:
  explicit PartitionDestination(const std::size_t num_partitions)
      : num_partitions_(num_partitions) {
    groups_.reserve(0x100);
  }

  inline std::size_t getNumPartitions() const {
    return num_partitions_;
  }

  PartitionGroup<Tuple>* allocateGroup() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!groups_.empty()) {
        auto *group = groups_.back().release();
        groups_.pop_back();
        return group;
      }
    }
    return new PartitionGroup<Tuple>(num_partitions_);
  }

  void returnGroup(PartitionGroup<Tuple> *group) {
    std::lock_guard<std::mutex> lock(mutex_);
    groups_.emplace_back(group);
  }

  template <typename Functor>
  inline void forEach(const std::size_t partition_id,
                      const Functor &functor) const {
    for (const auto &group : groups_) {
      group->forEach(partition_id, functor);
    }
  }

  std::size_t count() const {
    std::size_t total = 0;
    for (const auto &it : groups_) {
      total += it->count();
    }
    return total;
  }

 private:
  const std::size_t num_partitions_;

  std::vector<std::unique_ptr<PartitionGroup<Tuple>>> groups_;
  std::mutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(PartitionDestination);
};

}  // namespace project

#endif  // PROJECT_PARTITION_PARTITION_DESTINATION_HPP_
