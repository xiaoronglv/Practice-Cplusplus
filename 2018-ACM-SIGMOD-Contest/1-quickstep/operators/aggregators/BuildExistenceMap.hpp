#ifndef PROJECT_OPERATORS_AGGREGATOR_BUILD_EXISTENCE_MAP_HPP_
#define PROJECT_OPERATORS_AGGREGATOR_BUILD_EXISTENCE_MAP_HPP_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "partition/PartitionDestination.hpp"
#include "partition/PartitionDestinationBase.hpp"
#include "scheduler/Task.hpp"
#include "storage/ExistenceMap.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/ThreadSafePool.hpp"

#include "glog/logging.h"

namespace project {

class BuildExistenceMap {
 public:
  template <typename Callback>
  static void InvokeWithCallback(Task *ctx, Relation *relation,
                                 const attribute_id id,
                                 const Callback &callback);

 private:
  template <typename Callback>
  static void BuildWithSingleThread(Task *ctx, Relation *relation,
                                    const attribute_id id,
                                    const Range &domain,
                                    const Callback &callback);

  template <typename Callback>
  static void BuildWithTwoPhase(Task *ctx, Relation *relation,
                                const attribute_id id,
                                const Range &domain,
                                const Callback &callback);

  template <typename Callback>
  static void BuildWithPartitions(Task *ctx, Relation *relation,
                                  const attribute_id id,
                                  const Range &domain,
                                  const Callback &callback);

  template <typename Accessor>
  static void BuildLocal(const Accessor &accessor,
                         ExistenceMap *existence_map);

  static constexpr std::uint64_t kMaxRange = 0x1000000000ull;

  // TODO(robin-team): Move the two thresholds to some Flags.hpp
  static constexpr std::size_t kCardinalityThreshold = 1000000;
  static constexpr std::size_t kDomainSizeThreshold = 65536;

  static constexpr std::size_t kTwoPhaseBatchSize = 200000;

  DISALLOW_COPY_AND_ASSIGN(BuildExistenceMap);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Callback>
void BuildExistenceMap::InvokeWithCallback(Task *ctx, Relation *relation,
                                           const attribute_id id,
                                           const Callback &callback) {
  const auto &stat = relation->getStatistics().getAttributeStatistics(id);
  DCHECK(stat.hasMinValue());
  DCHECK(stat.hasMaxValue());

  const Range domain(stat.getMinValue(), stat.getMaxValue() + 1);
  CHECK_LE(domain.size(), kMaxRange);

  if (domain.size() <= kDomainSizeThreshold) {
    BuildWithTwoPhase(ctx, relation, id, domain, callback);
  } else if (relation->getIndexManager().hasRangedPartitions(id)) {
    BuildWithPartitions(ctx, relation, id, domain, callback);
  } else {
    BuildWithSingleThread(ctx, relation, id, domain, callback);
  }
}

template <typename Callback>
void BuildExistenceMap::BuildWithSingleThread(Task *ctx, Relation *relation,
                                              const attribute_id id,
                                              const Range &domain,
                                              const Callback &callback) {
  ctx->spawnLambdaTask([relation, id, domain, callback] {
    auto existence_map = std::make_unique<ExistenceMap>(domain);

    relation->forEachBlock(id, [&](auto accessor) {
      BuildLocal(*accessor, existence_map.get());
    });
    callback(existence_map);
  });
}

template <typename Callback>
void BuildExistenceMap::BuildWithTwoPhase(Task *ctx, Relation *relation,
                                          const attribute_id id,
                                          const Range &domain,
                                          const Callback &callback) {
  auto allocator = [domain] {
    return new ExistenceMap(domain);
  };
  using ExistenceMapPool = ThreadSafePool<ExistenceMap, decltype(allocator)>;

  auto existence_map_pool = std::make_shared<ExistenceMapPool>(allocator);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([relation, id, existence_map_pool](Task *internal) {
        relation->forEachBlockPiece(
            id, kTwoPhaseBatchSize,
            [&](auto accessor) {
          internal->spawnLambdaTask([accessor, existence_map_pool] {
            auto *existence_map = existence_map_pool->allocate();
            BuildLocal(*accessor, existence_map);
            existence_map_pool->push_back(existence_map);
          });
        });
      }),
      CreateLambdaTask([existence_map_pool, callback] {
        std::unique_ptr<ExistenceMap> existence_map(existence_map_pool->allocate());

        existence_map_pool->forEach([&](const auto &other) {
          existence_map->unionWith(other);
        });
        callback(existence_map);
      })));
}

template <typename Callback>
void BuildExistenceMap::BuildWithPartitions(Task *ctx, Relation *relation,
                                            const attribute_id id,
                                            const Range &domain,
                                            const Callback &callback) {
  const IndexManager &index_manager = relation->getIndexManager();
  DCHECK(index_manager.hasRangedPartitions(id));

  const PartitionDestinationBase &base = index_manager.getRangedPartitions(id);

  auto holder = std::make_shared<std::unique_ptr<ExistenceMap>>();
  auto &existence_map = *holder;

  existence_map = std::make_unique<ExistenceMap>(domain);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([relation, id, &base, &existence_map](Task *internal) {
        InvokeOnTypeIDForCppType(
            relation->getAttributeType(id).getTypeID(),
            [&](auto typelist) {
          using ValueType = typename decltype(typelist)::head;
          using PartitionDestinationType = PartitionDestination<ValueType>;

          const PartitionDestinationType &partitions =
              static_cast<const PartitionDestinationType&>(base);

          for (std::size_t i = 0; i < partitions.getNumPartitions(); ++i) {
            internal->spawnLambdaTask([i, &partitions, &existence_map] {
              partitions.forEach(i, [&](const auto &value) {
                existence_map->setBit(value);
              });
            });
          }
        });
      }),
      CreateLambdaTask([holder, callback](Task *internal) {
        callback(*holder);
      })));
}

template <typename Accessor>
void BuildExistenceMap::BuildLocal(const Accessor &accessor,
                                   ExistenceMap *existence_map) {
  for (std::size_t i = 0; i < accessor.getNumTuples(); ++i) {
    existence_map->setBit(accessor.at(i));
  }
}

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATOR_BUILD_EXISTENCE_MAP_HPP_
