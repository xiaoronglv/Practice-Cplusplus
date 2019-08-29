#include "operators/specializations/KeyCountVectorAccumulator02.hpp"

#include <atomic>
#include <cstddef>
#include <vector>

#include "operators/specializations/KeyCountVectorAccumulatorCommon.hpp"

#include "glog/logging.h"

namespace project {
namespace kcv {

void AccumulateXXX0(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator) {
  DCHECK(!accumulator->hasLookaheadFilter());

  InvokeOnKeyCountVectorsMakeSharedXXX(
      accumulator->getKeyCountVectors(),
      [&](const auto chain) -> void {
    accumulator->accumulate<true, false>(ctx, chain);
  });
}

}  // namespace kcv
}  // namespace project
