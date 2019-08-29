#ifndef PROJECT_OPERATORS_AGGREGATORS_BUILD_RANGED_PARTITIONS_HPP_
#define PROJECT_OPERATORS_AGGREGATORS_BUILD_RANGED_PARTITIONS_HPP_

#include <cstddef>
#include <memory>

#include "partition/PartitionDestinationBase.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

namespace project {

class Task;
class Relation;

class BuildRangedPartitions {
 public:
  static void Invoke(Task *ctx, Relation *relation, const attribute_id id);

 private:
  template <typename T>
  static void BuildInternal(Task *ctx, Relation *relation,
                            const attribute_id id);

  // TODO(robin-team): Move the two thresholds to some Flags.hpp
  static constexpr std::size_t kCardinalityThreshold = 15000000;
  static constexpr std::size_t kDomainSizeThreshold = 65536;

  static constexpr std::size_t kBatchSize = 1000000;

  DISALLOW_COPY_AND_ASSIGN(BuildRangedPartitions);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATORS_BUILD_RANGED_PARTITIONS_HPP_
