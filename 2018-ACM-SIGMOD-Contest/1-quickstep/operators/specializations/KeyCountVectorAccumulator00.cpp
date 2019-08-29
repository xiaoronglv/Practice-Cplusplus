#include "operators/specializations/KeyCountVectorAccumulator00.hpp"

#include <atomic>
#include <cstddef>
#include <vector>

#include "operators/specializations/KeyCountVectorAccumulatorCommon.hpp"

namespace project {
namespace kcv {

void AccumulateNA(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator) {
  InvokeOnKeyCountVectorsMakeSharedNA(
      accumulator->getKeyCountVectors(),
      [&](const auto cv) -> void {
    if (accumulator->hasLookaheadFilter()) {
      accumulator->accumulate<false, true>(ctx, cv);
    } else {
      accumulator->accumulate<false, false>(ctx, cv);
    }
  });
}

}  // namespace kcv
}  // namespace project
