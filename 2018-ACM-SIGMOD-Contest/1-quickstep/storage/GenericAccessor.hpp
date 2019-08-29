#ifndef PROJECT_GENERIC_ACCESSOR_HPP_
#define PROJECT_GENERIC_ACCESSOR_HPP_

#include "storage/ColumnAccessor.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/HashFilter.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class GenericAccessor {
 public:
  GenericAccessor(const ColumnAccessor &accessor,
                  const Range &range,
                  const TupleIdSequence *filter,
                  const HashFilter *lookahead_filter)
      : accessor_(accessor),
        range_(range),
        filter_(filter),
        lookahead_filter_(lookahead_filter) {}

  template <typename Functor>
  inline void forEach(const Functor &functor) const;

  template <typename Functor>
  inline void forEachPair(const Functor &functor) const;

 private:
  template <bool lookahead, typename Accessor, typename Functor>
  inline void forEachInternal(const Accessor &accessor,
                              const Functor &functor) const;

  const ColumnAccessor &accessor_;
  const Range range_;
  const TupleIdSequence *filter_;
  const HashFilter *lookahead_filter_;

  DISALLOW_COPY_AND_ASSIGN(GenericAccessor);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Functor>
inline void GenericAccessor::forEach(const Functor &functor) const {
  forEachPair([&](const tuple_id, const auto &value) {
    functor(value);
  });
}

template <typename Functor>
inline void GenericAccessor::forEachPair(const Functor &functor) const {
  InvokeOnColumnAccessor(
      &accessor_,
      [&](const auto *accessor) -> void {
    if (lookahead_filter_ != nullptr) {
      forEachInternal<true>(*accessor, functor);
    } else {
      forEachInternal<false>(*accessor, functor);
    }
  });
}

template <bool lookahead, typename Accessor, typename Functor>
inline void GenericAccessor::forEachInternal(const Accessor &accessor,
                                             const Functor &functor) const {
  DCHECK(!lookahead || lookahead_filter_ != nullptr);
  if (filter_ != nullptr) {
    for (const tuple_id tid : *filter_) {
      const auto value = accessor.at(tid);
      if (value < range_.begin() || value >= range_.end()) {
        continue;
      }
      if (lookahead && !lookahead_filter_->contains(value)) {
        continue;
      }
      functor(tid, value);
    }
  } else {
    for (std::size_t tid = 0; tid < accessor.getNumTuples(); ++tid) {
      const auto value = accessor.at(tid);
      if (value < range_.begin() || value >= range_.end()) {
        continue;
      }
      if (lookahead && !lookahead_filter_->contains(value)) {
        continue;
      }
      functor(tid, value);
    }
  }
}

}  // namespace project

#endif  // PROJECT_GENERIC_ACCESSOR_HPP_
