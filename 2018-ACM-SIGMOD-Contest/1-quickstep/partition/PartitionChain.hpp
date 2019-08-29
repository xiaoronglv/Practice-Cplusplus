#ifndef PROJECT_PARTITION_PARTITION_CHAIN_HPP_
#define PROJECT_PARTITION_PARTITION_CHAIN_HPP_

#include <cstddef>
#include <cstring>
#include <list>
#include <string>
#include <utility>

#include "partition/PartitionBlockAllocator.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

template <typename Tuple>
class PartitionChain {
 public:
  PartitionChain() {}

  inline void init(PartitionBlockAllocator<Tuple> *allocator) {
    allocator_ = allocator;

    current_.reset(allocator_->allocateBlock(),
                   allocator_->getNumTupleSlotsPerBlock(),
                   nullptr /* previous */);
  }

  inline void append(const Tuple *tuple) {
    std::memcpy(allocateTupleSlot(), tuple, sizeof(Tuple));
  }

  inline void append(const Tuple &tuple) {
    append(&tuple);
  }

  template <typename Functor>
  inline void forEach(const Functor &functor) const {
    Block it = current_;
    while (true) {
      for (std::size_t i = 0; i < *it.length; ++i) {
        functor(it.at(i));
      }
      if (it.hasNext()) {
        it.moveToNext();
      } else {
        break;
      }
    }
  }

 private:
  inline Tuple* allocateTupleSlot() {
    if (current_.full()) {
      newBlock();
    }
    return current_.allocateTupleSlot();
  }

  inline void newBlock() {
    current_.reset(allocator_->allocateBlock(),
                   allocator_->getNumTupleSlotsPerBlock(),
                   current_.data);
  }

  struct Block {
    inline void reset(void *memory,
                      const std::size_t num_slots,
                      void *last) {
      data = static_cast<Tuple*>(memory);
      memory = data + num_slots;
      capacity = num_slots;

      length = reinterpret_cast<std::size_t*>(memory);
      *length = 0;
      memory = length + 1;

      *reinterpret_cast<void**>(memory) = last;
      next = last;
    }

    inline bool full() const {
      return *length == capacity;
    }

    inline Tuple& at(const std::size_t pos) const {
      DCHECK_LT(pos, *length);
      return data[pos];
    }

    inline Tuple* allocateTupleSlot() {
      DCHECK_LT(*length, capacity);
      return data + (*length)++;
    }

    inline bool hasNext() const {
      return next != nullptr;
    }

    inline void moveToNext() {
      data = static_cast<Tuple*>(next);
      next = data + capacity;

      length = reinterpret_cast<std::size_t*>(next);
      next = length + 1;

      next = *reinterpret_cast<void**>(next);
    }

    Tuple *data;
    std::size_t *length;

    std::size_t capacity;
    void *next;
  };

  Block current_;
  PartitionBlockAllocator<Tuple> *allocator_;

  DISALLOW_COPY_AND_ASSIGN(PartitionChain);
};

}  // namespace project

#endif  // PROJECT_PARTITION_PARTITION_CHAIN_HPP_
