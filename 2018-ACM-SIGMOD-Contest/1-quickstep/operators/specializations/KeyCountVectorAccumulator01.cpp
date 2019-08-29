#include "operators/specializations/KeyCountVectorAccumulator01.hpp"

#include <atomic>
#include <cstddef>
#include <vector>

#include "operators/utility/KeyCountVector.hpp"
#include "operators/specializations/KeyCountVectorAccumulatorCommon.hpp"

namespace project {
namespace kcv {

void AccumulateX(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator) {
  InvokeOnKeyCountVectorsMakeSharedX(
      accumulator->getKeyCountVectors(),
      [&](const auto cv) -> void {
    if (accumulator->hasLookaheadFilter()) {
      accumulator->accumulate<false, true>(ctx, cv);
    } else {
      accumulator->accumulate<false, false>(ctx, cv);
    }
  });
}

void AccumulateXX(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator) {
  InvokeOnKeyCountVectorsMakeSharedXX(
      accumulator->getKeyCountVectors(),
      [&](const auto chain) -> void {
    if (accumulator->hasLookaheadFilter()) {
      accumulator->accumulate<true, true>(ctx, chain);
    } else {
      accumulator->accumulate<true, false>(ctx, chain);
    }
  });
}

}  // namespace kcv
}  // namespace project
