#include "operators/expressions/TruePredicate.hpp"

#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

TupleIdSequence* TruePredicate::getAllMatches(const StorageBlock &block,
                                              TupleIdSequence *filter) const {
  LOG(FATAL) << "Not implemented";
}

Range TruePredicate::reduceRange(const Range &range, bool *exactness) const {
  return range;
}

bool TruePredicate::impliesExactRange(const attribute_id id) const {
  return true;
}

}  // namespace project
