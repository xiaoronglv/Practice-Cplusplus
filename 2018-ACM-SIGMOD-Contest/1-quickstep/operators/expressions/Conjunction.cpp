#include "operators/expressions/Conjunction.hpp"

#include <memory>

#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/Range.hpp"

namespace project {

TupleIdSequence* Conjunction::getAllMatches(const StorageBlock &block,
                                           TupleIdSequence *filter) const {
  std::unique_ptr<TupleIdSequence> matches(filter);
  for (const auto &operand : operands_) {
    matches.reset(operand->getAllMatches(block, matches.release()));
  }
  return matches.release();
}

Range Conjunction::reduceRange(const Range &range, bool *exactness) const {
  Range output = range;
  for (const auto &operand : operands_) {
    output = operand->reduceRange(output, exactness);
  }
  return output;
}

bool Conjunction::impliesExactRange(const attribute_id id) const {
  for (const auto &operand : operands_) {
    if (!operand->impliesExactRange(id)) {
      return false;
    }
  }
  return true;
}

}  // namespace project
