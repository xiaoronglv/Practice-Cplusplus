#ifndef PROJECT_STORAGE_TUPLE_ID_SEQUENCE_HPP_
#define PROJECT_STORAGE_TUPLE_ID_SEQUENCE_HPP_

#include <cstddef>
#include <limits>
#include <vector>

#include "storage/StorageTypedefs.hpp"
#include "utility/BitVector.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class TupleIdSequence {
 public:
  class ConstIterator {
   public:
    ConstIterator()
        : bitvector_(nullptr) {
    }

    inline tuple_id operator*() const {
      DCHECK(bitvector_ != nullptr);
      DCHECK_LT(current_position_, bitvector_->size());
      return current_position_;
    }

    inline ConstIterator& operator++() {
      DCHECK(bitvector_ != nullptr);
      const std::size_t search_pos = current_position_ + 1;
      current_position_ =
          (search_pos < bitvector_->size()) ? bitvector_->firstOne(search_pos)
                                            : bitvector_->size();
      return *this;
    }

    inline ConstIterator operator++(int) {
      ConstIterator result(*this);
      ++(*this);
      return result;
    }

    inline bool operator==(const ConstIterator& other) const {
      return ((bitvector_ == other.bitvector_)
              && (current_position_ == other.current_position_));
    }

    inline bool operator!=(const ConstIterator& other) const {
      return !(*this == other);
    }

   private:
    friend class TupleIdSequence;

    ConstIterator(const BitVector *bitvector,
                  const std::size_t initial_position)
        : bitvector_(bitvector),
          current_position_(initial_position) {
    }

    const BitVector *bitvector_;
    std::size_t current_position_;
  };

  typedef ConstIterator const_iterator;

  explicit TupleIdSequence(const tuple_id length)
      : internal_bitvector_(length) {
  }

  inline std::size_t getNumTuples() const {
    return internal_bitvector_.onesCount();
  }

  inline std::size_t length() const {
    return internal_bitvector_.size();
  }

  inline void set(const tuple_id tuple) const {
    DCHECK_GE(tuple, 0);
    DCHECK_LT(static_cast<std::size_t>(tuple), internal_bitvector_.size());
    internal_bitvector_.setBit(tuple);
  }

  inline const_iterator begin() const {
    return const_iterator(&internal_bitvector_, internal_bitvector_.firstOne());
  }

  inline const_iterator before_begin() const {
    return const_iterator(&internal_bitvector_, std::numeric_limits<std::size_t>::max());
  }

  inline const_iterator end() const {
    return const_iterator(&internal_bitvector_, internal_bitvector_.size());
  }

  inline tuple_id front() const {
    return internal_bitvector_.firstOne();
  }

  const BitVector& getInternalBitVector() const {
    return internal_bitvector_;
  }

 private:
  BitVector internal_bitvector_;

  DISALLOW_COPY_AND_ASSIGN(TupleIdSequence);
};

typedef std::vector<tuple_id> OrderedTupleIdSequence;

}  // namespace project

#endif  // PROJECT_STORAGE_TUPLE_ID_SEQUENCE_HPP_
