#ifndef PROJECT_STORAGE_FOREIGN_KEY_INDEX_HPP_
#define PROJECT_STORAGE_FOREIGN_KEY_INDEX_HPP_

#include <cstddef>
#include <cstdint>
#include <utility>

#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/ScopedArray.hpp"

namespace project {

class ForeignKeyIndex {
 public:
  virtual ~ForeignKeyIndex() {}

  virtual std::size_t getTypeSize() const = 0;

  virtual std::size_t getMaxCount() const = 0;

  virtual Range getRange() const = 0;

 protected:
  ForeignKeyIndex() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(ForeignKeyIndex);
};

template <typename T>
class ForeignKeyIndexImpl : public ForeignKeyIndex {
 public:
  typedef T ValueType;
  typedef std::pair<tuple_id, std::uint32_t> Slot;
  typedef std::pair<T, tuple_id> Bucket;

  std::size_t getTypeSize() const override {
    return sizeof(T);
  }

  ForeignKeyIndexImpl(const T base,
                      const std::size_t length,
                      const std::size_t max_count,
                      Slot *slots,
                      Bucket *buckets)
      : base_(base),
        length_(length),
        max_count_(max_count),
        slots_(slots),
        buckets_(buckets) {}

  std::size_t getMaxCount() const override {
    return max_count_;
  }

  Range getRange() const override {
    return Range(base_, base_ + length_);
  }


  inline T base() const {
    return base_;
  }

  inline std::size_t length() const {
    return length_;
  }

  inline const Slot* slots() const {
    return slots_.get();
  }

  inline const Bucket* buckets() const {
    return buckets_.get();
  }

  inline Slot slotAt(const T value) const {
    if (value < base_ || value >= base_ + length_) {
      return Slot(kInvalidTupleID, 0);
    }
    return slots_[value - base_];
  }

  inline const Slot& slotAtUnchecked(const T value) const {
    DCHECK_LE(base_, value);
    DCHECK_GT(base_ + length_, value);
    return slots_[value - base_];
  }

  inline const Bucket& bucketAt(const std::size_t pos) const {
    return buckets_[pos];
  }

 private:
  const T base_;
  const std::size_t length_;
  const std::size_t max_count_;
  const ScopedArray<Slot> slots_;
  const ScopedArray<Bucket> buckets_;

  DISALLOW_COPY_AND_ASSIGN(ForeignKeyIndexImpl);
};

}  // namespace project

#endif  // PROJECT_STORAGE_FOREIGN_KEY_INDEX_HPP_

