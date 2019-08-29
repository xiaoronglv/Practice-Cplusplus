#ifndef PROJECT_OPERATORS_UTILITY_FOREIGN_KEY_SLOT_ACCESSOR_HPP_
#define PROJECT_OPERATORS_UTILITY_FOREIGN_KEY_SLOT_ACCESSOR_HPP_

#include <cstddef>
#include <cstdint>
#include <tuple>
#include <utility>

#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

template <typename T>
class ForeignKeySlotAccessor {
 public:
  typedef T ValueType;
  typedef std::pair<tuple_id, std::uint32_t> Slot;

  ForeignKeySlotAccessor(const T base,
                         const T length,
                         const Slot *slots)
      : base_(base),
        length_(length),
        slots_(slots) {}

  inline std::size_t length() const {
    return length_;
  }

  inline T begin() const {
    return base_;
  }

  inline T end() const {
    return base_ + length_;
  }

  inline std::uint32_t at(const T value) const {
    DCHECK_LE(begin(), value);
    DCHECK_GT(end(), value);
    return slots_[value - base_].second;
  }

  inline std::uint32_t atChecked(const T value) const {
    if (value < begin() || value >= end()) {
      return 0;
    }
    return slots_[value - base_].second;
  }

 private:
  const T base_;
  const T length_;
  const Slot *slots_;

  DISALLOW_COPY_AND_ASSIGN(ForeignKeySlotAccessor);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_UTILITY_FOREIGN_KEY_SLOT_ACCESSOR_HPP_
