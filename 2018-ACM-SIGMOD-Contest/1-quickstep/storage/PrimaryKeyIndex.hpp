#ifndef PROJECT_STORAGE_PRIMARY_KEY_INDEX_HPP_
#define PROJECT_STORAGE_PRIMARY_KEY_INDEX_HPP_

#include <cstddef>
#include <cstdint>
#include <utility>

#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/ScopedArray.hpp"

namespace project {

class PrimaryKeyIndex {
 public:
  virtual ~PrimaryKeyIndex() {}

  virtual std::size_t getTypeSize() const = 0;

  virtual Range getRange() const = 0;

  virtual tuple_id lookupVirtual(const std::uint64_t value) const = 0;

 protected:
  PrimaryKeyIndex() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(PrimaryKeyIndex);
};

template <typename T>
class PrimaryKeyIndexImpl : public PrimaryKeyIndex {
 public:
  typedef T ValueType;

  std::size_t getTypeSize() const override {
    return sizeof(T);
  }

  PrimaryKeyIndexImpl(const T base,
                      const std::size_t length,
                      ScopedArray<tuple_id> &&slots,
                      const T *data)
      : base_(base),
        length_(length),
        slots_(std::move(slots)),
        data_(data) {}

  Range getRange() const override {
    return Range(base_, base_ + length_);
  }

  tuple_id lookupVirtual(const std::uint64_t value) const override {
    return slotAt(value);
  }

  inline tuple_id slotAt(const T value) const {
    if (value < base_ || value >= base_ + length_) {
      return kInvalidTupleID;
    }
    return slots_[value - base_];
  }

  inline tuple_id slotAtUnchecked(const T value) const {
    DCHECK_LE(base_, value);
    DCHECK_GT(base_ + length_, value);
    return slots_[value - base_];
  }

 private:
  const T base_;
  const std::size_t length_;
  const ScopedArray<tuple_id> slots_;
  const T *data_;

  DISALLOW_COPY_AND_ASSIGN(PrimaryKeyIndexImpl);
};

}  // namespace project

#endif  // PROJECT_STORAGE_PRIMARY_KEY_INDEX_HPP_

