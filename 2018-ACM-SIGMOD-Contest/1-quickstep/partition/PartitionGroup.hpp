#ifndef PROJECT_PARTITION_PARTITION_GROUP_HPP_
#define PROJECT_PARTITION_PARTITION_GROUP_HPP_

#include <cstddef>
#include <vector>

#include "partition/PartitionBlockAllocator.hpp"
#include "partition/PartitionChain.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

template <typename Tuple>
class PartitionGroup {
 public:
  explicit PartitionGroup(const std::size_t num_partitions)
      : partitions_(num_partitions),
        allocator_(num_partitions) {
    for (auto &partition : partitions_) {
      partition.init(&allocator_);
    }
  }

  inline void append(const std::size_t partition_id, const Tuple *tuple) {
    DCHECK_LT(partition_id, partitions_.size());
    partitions_[partition_id].append(tuple);
  }

  inline void append(const std::size_t partition_id, const Tuple &tuple) {
    append(partition_id, &tuple);
  }

  template <typename Functor>
  inline void forEach(const std::size_t partition_id,
                      const Functor &functor) {
    DCHECK_LT(partition_id, partitions_.size());
    partitions_[partition_id].forEach(functor);
  }

  std::size_t count() const {
    std::size_t total = 0;
    for (const auto &partition : partitions_) {
      total += partition.count();
    }
    return total;
  }

 private:
  std::vector<PartitionChain<Tuple>> partitions_;

  PartitionBlockAllocator<Tuple> allocator_;

  DISALLOW_COPY_AND_ASSIGN(PartitionGroup);
};

}  // namespace project

#endif  // PROJECT_PARTITION_PARTITION_GROUP_HPP_
