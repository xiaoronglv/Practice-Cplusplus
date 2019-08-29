#include "operators/specializations/KeyCountVectorAccumulator03.hpp"

#include <atomic>
#include <cstddef>
#include <vector>

#include "operators/specializations/KeyCountVectorAccumulatorCommon.hpp"

namespace project {
namespace kcv {

void AccumulateXXX1(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator) {
  DCHECK(accumulator->hasLookaheadFilter());

  InvokeOnKeyCountVectorsMakeSharedXXX(
      accumulator->getKeyCountVectors(),
      [&](const auto chain) -> void {
    accumulator->accumulate<true, true>(ctx, chain);
  });
}

}  // namespace kcv
}  // namespace project
