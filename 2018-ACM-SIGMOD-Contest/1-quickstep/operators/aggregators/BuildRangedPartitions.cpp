#include "operators/aggregators/BuildRangedPartitions.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>

#include "partition/PartitionExecutor.hpp"
#include "partition/RangeSegmentation.hpp"
#include "scheduler/Task.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/BitManipulation.hpp"
#include "utility/Range.hpp"

namespace project {

namespace {

inline std::size_t CalculateSegmentWidth(const Range &domain) {
  // TODO(robin-team): Find best width.
  const int digits = MostSignificantBit(domain.size());
  return std::max(64ull, (1ull << digits) >> 6);
}

}  // namespace

template <typename T>
void BuildRangedPartitions::BuildInternal(Task *ctx, Relation *relation,
                                          const attribute_id id) {
  const AttributeStatistics &stat =
      relation->getStatistics().getAttributeStatistics(id);
  DCHECK(stat.hasMinValue());
  DCHECK(stat.hasMaxValue());

  const Range domain(stat.getMinValue(), stat.getMaxValue() + 1);

  if (domain.size() <= kDomainSizeThreshold) {
    return;
  }

  std::shared_ptr<PartitionExecutor<T, RangeSegmentation>> executor =
      std::make_shared<PartitionExecutor<T, RangeSegmentation>>(
          RangeSegmentation(domain, CalculateSegmentWidth(domain)));

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([relation, id, executor](Task *internal) {
        relation->forEachBlockPiece(
            id, kBatchSize,
            [&](auto accessor) {
          internal->spawnLambdaTask([accessor, executor] {
            executor->insert(*accessor);
          });
        });
      }),
      CreateLambdaTask([relation, id, executor](Task *internal) {
        relation->getIndexManagerMutable()
                ->setRangedPartitions(id, executor->releaseOutput());
      })));
}

void BuildRangedPartitions::Invoke(
    Task *ctx, Relation *relation, const attribute_id id) {
  if (relation->getNumTuples() <= kCardinalityThreshold) {
    return;
  }

  InvokeOnTypeIDForCppType(
      relation->getAttributeType(id).getTypeID(),
      [&](auto typelist) {
    using T = typename decltype(typelist)::head;
    BuildInternal<T>(ctx, relation, id);
  });
}

}  // namespace project
