#ifndef PROJECT_STORAGE_INDEX_MANAGER_HPP_
#define PROJECT_STORAGE_INDEX_MANAGER_HPP_

#include <cstddef>
#include <memory>
#include <vector>

#include "operators/utility/KeyCountVector.hpp"
#include "storage/ExistenceMap.hpp"
#include "storage/ForeignKeyIndex.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/StorageTypedefs.hpp"
#include "partition/PartitionDestinationBase.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class IndexManager {
 public:
  explicit IndexManager(const std::size_t num_attributes)
      : existence_maps_(num_attributes),
        primary_key_indexes_(num_attributes),
        foreign_key_indexes_(num_attributes),
        ranged_partitions_(num_attributes),
        key_count_vectors_(num_attributes) {}

  bool hasExistenceMap(const attribute_id id) const {
    DCHECK_LT(id, existence_maps_.size());
    return existence_maps_[id] != nullptr;
  }

  const ExistenceMap& getExistenceMap(const attribute_id id) const {
    DCHECK_LT(id, existence_maps_.size());
    return *existence_maps_[id];
  }

  void setExistenceMap(const attribute_id id, ExistenceMap *existence_map) {
    DCHECK_LT(id, existence_maps_.size());
    existence_maps_[id].reset(existence_map);
  }

  bool hasPrimaryKeyIndex(const attribute_id id) const {
    DCHECK_LT(id, primary_key_indexes_.size());
    return primary_key_indexes_[id] != nullptr;
  }

  const PrimaryKeyIndex& getPrimaryKeyIndex(const attribute_id id) const {
    DCHECK(hasPrimaryKeyIndex(id));
    return *primary_key_indexes_[id];
  }

  void setPrimaryKeyIndex(const attribute_id id, PrimaryKeyIndex *index) {
    DCHECK(!hasPrimaryKeyIndex(id));
    primary_key_indexes_[id].reset(index);
  }

  bool hasForeignKeyIndex(const attribute_id id) const {
    DCHECK_LT(id, foreign_key_indexes_.size());
    return foreign_key_indexes_[id] != nullptr;
  }

  const ForeignKeyIndex& getForeignKeyIndex(const attribute_id id) const {
    DCHECK(hasForeignKeyIndex(id));
    return *foreign_key_indexes_[id];
  }

  void setForeignKeyIndex(const attribute_id id, ForeignKeyIndex *index) {
    DCHECK(!hasForeignKeyIndex(id));
    foreign_key_indexes_[id].reset(index);
  }

  bool hasRangedPartitions(const attribute_id id) const {
    DCHECK_LT(id, ranged_partitions_.size());
    return ranged_partitions_[id] != nullptr;
  }

  const PartitionDestinationBase& getRangedPartitions(const attribute_id id) const {
    DCHECK(hasRangedPartitions(id));
    return *ranged_partitions_[id];
  }

  void setRangedPartitions(const attribute_id id, PartitionDestinationBase *base) {
    DCHECK(!hasRangedPartitions(id));
    ranged_partitions_[id].reset(base);
  }

  void destroyRangedPartitions(const attribute_id id) {
    DCHECK_LT(id, ranged_partitions_.size());
    ranged_partitions_[id].reset();
  }

  bool hasKeyCountVector(const attribute_id id) const {
    DCHECK_LT(id, key_count_vectors_.size());
    return key_count_vectors_[id] != nullptr;
  }

  const KeyCountVector& getKeyCountVector(const attribute_id id) const {
    DCHECK(hasKeyCountVector(id));
    return *key_count_vectors_[id];
  }

  void setKeyCountVector(const attribute_id id, KeyCountVector *kcv) {
    DCHECK(!hasKeyCountVector(id));
    key_count_vectors_[id].reset(kcv);
  }

 private:
  std::vector<std::unique_ptr<ExistenceMap>> existence_maps_;
  std::vector<std::unique_ptr<PrimaryKeyIndex>> primary_key_indexes_;
  std::vector<std::unique_ptr<ForeignKeyIndex>> foreign_key_indexes_;
  std::vector<std::unique_ptr<PartitionDestinationBase>> ranged_partitions_;
  std::vector<std::unique_ptr<KeyCountVector>> key_count_vectors_;

  DISALLOW_COPY_AND_ASSIGN(IndexManager);
};

}  // namespace project

#endif  // PROJECT_STORAGE_INDEX_MANAGER_HPP_
