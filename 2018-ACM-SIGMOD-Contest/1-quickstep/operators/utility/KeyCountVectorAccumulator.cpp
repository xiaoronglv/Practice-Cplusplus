#include "operators/utility/KeyCountVectorAccumulator.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "operators/specializations/KeyCountVectorAccumulator00.hpp"
#include "operators/specializations/KeyCountVectorAccumulator01.hpp"
#include "operators/specializations/KeyCountVectorAccumulator02.hpp"
#include "operators/specializations/KeyCountVectorAccumulator03.hpp"
#include "operators/specializations/KeyCountVectorAccumulatorCommon.hpp"
#include "operators/utility/MultiwayJoinContext.hpp"
#include "scheduler/Task.hpp"

#include "glog/logging.h"

namespace project {

void KeyCountVectorAccumulator::Invoke(Task *ctx,
                                       MultiwayJoinContext *context,
                                       const Range &domain,
                                       std::vector<const KeyCountVector*> &&kcvs,
                                       const HashFilter *lookahead_filter,
                                       std::atomic<bool> *is_null) {
  if (context->aggregate_expressions.empty()) {
    return;
  }

  auto builder =
      std::make_shared<kcv::KeyCountVectorAccumulatorCommon>(
          context, domain, lookahead_filter, std::move(kcvs), is_null);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([builder](Task *internal) {
        switch (builder->getKeyCountVectors().size()) {
          case 0:
            kcv::AccumulateNA(internal, builder.get());
            break;
          case 1:
            kcv::AccumulateX(internal, builder.get());
            break;
          case 2:
            kcv::AccumulateXX(internal, builder.get());
            break;
          case 3:
            if (builder->hasLookaheadFilter()) {
              kcv::AccumulateXXX1(internal, builder.get());
            } else {
              kcv::AccumulateXXX0(internal, builder.get());
            }
            break;
          default:
            LOG(FATAL) << "Cannot handle more than 3 key count vectors";
        }
      }),
      CreateLambdaTask([builder] {})));
}

}  // namespace project
