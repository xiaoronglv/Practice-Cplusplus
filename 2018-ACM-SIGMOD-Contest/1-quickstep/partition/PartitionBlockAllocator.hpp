#ifndef PROJECT_PARTITION_PARTITION_BLOCK_ALLOCATOR_HPP_
#define PROJECT_PARTITION_PARTITION_BLOCK_ALLOCATOR_HPP_

#include <cstddef>
#include <vector>

#include "utility/Macros.hpp"
#include "utility/ScopedArray.hpp"

namespace project {

template <typename Tuple>
class PartitionBlockAllocator {
 public:
  explicit PartitionBlockAllocator(const std::size_t num_partitions)
      : batch_size_(allocate_factor_ * num_partitions) {
    newBatch();
  }

  inline std::size_t getNumTupleSlotsPerBlock() const {
    constexpr std::size_t kExtraBytes = sizeof(std::size_t) + sizeof(void*);
    constexpr std::size_t kTupleBytes = sizeof(Tuple);
    constexpr std::size_t kTupleSlotsOccupied =
        (kExtraBytes + kTupleBytes - 1) / kTupleBytes;

    return block_size_ - kTupleSlotsOccupied;
  }

  inline void* allocateBlock() {
    if (current_ == batch_size_) {
      newBatch();
    }
    return batches_.back().get() + (current_++ * block_size_);
  }

 private:
  inline void newBatch() {
    batches_.emplace_back(batch_size_ * block_size_);
    current_ = 0;
  }

  const std::size_t block_size_ = 0x1000;

  const std::size_t allocate_factor_ = 16;
  const std::size_t batch_size_;

  std::size_t current_;

  std::vector<ScopedArray<Tuple>> batches_;

  DISALLOW_COPY_AND_ASSIGN(PartitionBlockAllocator);
};

}  // namespace project

#endif  // PROJECT_PARTITION_PARTITION_BLOCK_ALLOCATOR_HPP_
