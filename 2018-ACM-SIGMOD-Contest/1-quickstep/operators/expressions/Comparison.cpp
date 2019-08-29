#include "operators/expressions/Comparison.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "operators/expressions/ScalarAttribute.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

namespace {

template <typename Comparator>
void GetAllMatchesInternalWithFilter(const Comparator &comparator,
                                     const TupleIdSequence &filter,
                                     TupleIdSequence *matches) {
  for (const tuple_id tuple : filter) {
    if (comparator(tuple)) {
      matches->set(tuple);
    }
  }
}

template <typename Comparator>
void GetAllMatchesInternal(const Comparator &comparator,
                           const TupleIdSequence *filter,
                           TupleIdSequence *matches) {
  if (filter != nullptr) {
    GetAllMatchesInternalWithFilter(comparator, *filter, matches);
    return;
  }

  const tuple_id num_tuples = matches->length();
  for (tuple_id i = 0; i < num_tuples; ++i) {
    if (comparator(i)) {
      matches->set(i);
    }
  }
}

}  // namespace

TupleIdSequence* Comparison::getAllMatches(const StorageBlock &block,
                                           TupleIdSequence *filter) const {
  std::unique_ptr<TupleIdSequence> holder(filter);
  std::unique_ptr<TupleIdSequence> matches =
      std::make_unique<TupleIdSequence>(block.getNumTuples());

  invokeOnAnyComparison(
      block,
      [&](auto comparator) {
    GetAllMatchesInternal(*comparator, filter, matches.get());
  });

  return matches.release();
}

Range Comparison::reduceRange(const Range &range, bool *exactness) const {
  DCHECK(left_->getScalarType() == ScalarType::kAttribute);

  if (range.size() == 0) {
    return range;
  }

  if (right_->getScalarType() != ScalarType::kLiteral) {
    *exactness = false;
    return range;
  }

  const std::uint64_t literal =
      static_cast<const ScalarLiteral&>(*right_).value();

  const std::uint64_t begin = range.begin();
  const std::uint64_t end = range.end();

  switch (comparison_type_) {
    case ComparisonType::kEqual:
      if (literal >= begin && literal < end) {
        return Range(literal, literal+1);
      }
      break;
    case ComparisonType::kLess:
      if (literal > begin) {
        return Range(begin, std::min(literal, end));
      }
      break;
    case ComparisonType::kGreater:
      if (literal+1 < end) {
        return Range(std::max(begin, literal+1), end);
      }
      break;
    default:
      break;
  }
  return Range();
}

bool Comparison::impliesExactRange(const attribute_id id) const {
  DCHECK(left_->getScalarType() == ScalarType::kAttribute);

  if (right_->getScalarType() != ScalarType::kLiteral) {
    return false;
  }

  return static_cast<const ScalarAttribute&>(*left_).attribute()->id() == id;
}


}  // namespace project
