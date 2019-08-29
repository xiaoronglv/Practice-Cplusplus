#ifndef PROJECT_STORAGE_RELATION_HPP_
#define PROJECT_STORAGE_RELATION_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "storage/Attribute.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/IndexManager.hpp"
#include "storage/RelationStatistics.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

// TODO(robin-team): Move this enum class to a suitable place.
enum class EarlyTerminationStatus {
  kYes,
  kNo
};

class Relation {
 public:
  Relation(const std::vector<const Type*> &attribute_types,
           const bool is_temporary);

  bool isTemporary() const {
    return is_temporary_;
  }

  std::size_t getNumAttributes() const {
    return attributes_.size();
  }

  std::size_t getNumBlocks() const {
    return blocks_.size();
  }

  std::size_t getNumTuples() const {
    return statistics_.getNumTuples();
  }

  const Attribute& getAttribute(const attribute_id id) const {
    DCHECK_LT(id, getNumAttributes());
    return *attributes_[id];
  }

  const Type& getAttributeType(const attribute_id id) const {
    DCHECK_LT(id, getNumAttributes());
    return attributes_[id]->getType();
  }

  bool isPrimaryKey(const attribute_id id) const {
    DCHECK_LT(id, getNumAttributes());
    const AttributeStatistics &stat = statistics_.getAttributeStatistics(id);
    return stat.hasNumDistinctValues() &&
           stat.getNumDistinctValues() == statistics_.getNumTuples();
  }

  relation_id getID() const {
    return id_;
  }

  void setID(const relation_id id) {
    id_ = id;
  }

  std::string getName() const {
    return "r" + std::to_string(id_);
  }

  std::string getAttributeName(const attribute_id id) const {
    DCHECK_LT(id, getNumAttributes());
    return attributes_[id]->getName();
  }

  std::string getAttributeFullName(const attribute_id id) const {
    DCHECK_LT(id, getNumAttributes());
    return getName() + "." + attributes_[id]->getName();
  }

  void addBlock(StorageBlock *block) {
    if (block->getNumTuples() == 0) {
      delete block;
      return;
    }
    {
      std::lock_guard<std::mutex> lock(block_mutex_);
      blocks_.emplace_back(block);
    }

    statistics_.increaseNumTuples(block->getNumTuples());
  }

  const StorageBlock& getBlock(const std::size_t block_id) const {
    DCHECK_LT(block_id, blocks_.size());
    return *blocks_[block_id];
  }

  StorageBlock& getBlockMutable(const std::size_t block_id) {
    DCHECK_LT(block_id, blocks_.size());
    return *blocks_[block_id];
  }

  const RelationStatistics& getStatistics() const {
    return statistics_;
  }

  RelationStatistics* getStatisticsMutable() {
    return &statistics_;
  }

  const IndexManager& getIndexManager() const {
    return index_manager_;
  }

  IndexManager* getIndexManagerMutable() {
    return &index_manager_;
  }

  std::string getBriefSummary() const;

  template <typename Functor>
  void forEachBlock(const Functor &functor) const;

  template <typename Functor>
  void forEachBlock(const attribute_id column_id,
                    const Functor &functor) const;

  template <typename Functor>
  void forEachBlockPiece(const std::size_t batch_size,
                         const Functor &functor) const;

  template <typename Functor>
  void forEachBlockPiece(const attribute_id column_id,
                         const std::size_t batch_size,
                         const Functor &functor) const;

  template <typename Functor>
  void forEachBlockEarlyTermination(const attribute_id column_id,
                                    const Functor &functor) const;

  template <typename Functor>
  void forSingletonBlock(const Functor &functor) const;

  template <typename Functor>
  void forSingletonBlock(const attribute_id column_id,
                         const Functor &functor) const;

  // For column compressing at the pre-processing phase.
  void replaceColumnn(const attribute_id id,
                      const Type &type,
                      ColumnVector *column) {
    DCHECK_EQ(1u, blocks_.size());
    DCHECK(blocks_.front()->getStorageBlockType() == StorageBlock::kColumnStore);

    static_cast<ColumnStoreBlock&>(*blocks_.front()).replaceColumn(id, column);
    attributes_[id]->setType(type);
  }

 private:
  const bool is_temporary_;
  relation_id id_;

  std::vector<std::unique_ptr<Attribute>> attributes_;
  std::vector<std::unique_ptr<StorageBlock>> blocks_;

  RelationStatistics statistics_;
  IndexManager index_manager_;

  mutable std::mutex block_mutex_;

  DISALLOW_COPY_AND_ASSIGN(Relation);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Functor>
void Relation::forEachBlock(const Functor &functor) const {
  for (std::size_t block_id = 0; block_id < getNumBlocks(); ++block_id) {
    const StorageBlock &block = getBlock(block_id);
    DCHECK_GT(block.getNumTuples(), static_cast<std::size_t>(0));

    // TODO(robin-team): Handle more block types (e.g. RowStoreBlock).
    DCHECK(block.getStorageBlockType() == StorageBlock::kColumnStore);
    const ColumnStoreBlock &csb = static_cast<const ColumnStoreBlock&>(block);
    functor(csb);
  }
}

template <typename Functor>
void Relation::forEachBlock(const attribute_id column_id,
                            const Functor &functor) const {
  forEachBlock([&](const auto &block) {
    InvokeOnColumnAccessorMakeShared(
        block.createColumnAccessor(column_id), functor);
  });
}

template <typename Functor>
void Relation::forEachBlockPiece(const std::size_t batch_size,
                                 const Functor &functor) const {
  forEachBlock([&](const auto &block) {
    using BlockType = std::remove_reference_t<decltype(block)>;
    const RangeSplitter splitter =
        RangeSplitter::CreateWithPartitionLength(0, block.getNumTuples(), batch_size);
    for (const Range range : splitter) {
      std::shared_ptr<StorageBlock> subset(block.createSubset(range));
      functor(std::static_pointer_cast<BlockType>(subset));
    }
  });
}

template <typename Functor>
void Relation::forEachBlockPiece(const attribute_id column_id,
                                 const std::size_t batch_size,
                                 const Functor &functor) const {
  forEachBlock([&](const auto &block) {
    std::unique_ptr<ColumnAccessor> column(block.createColumnAccessor(column_id));

    const RangeSplitter splitter =
        RangeSplitter::CreateWithPartitionLength(0, block.getNumTuples(), batch_size);
    for (const Range range : splitter) {
      std::unique_ptr<ColumnAccessor> subset(column->createSubset(range));
      InvokeOnColumnAccessorMakeShared(subset.release(), functor);
    }
  });
}

template <typename Functor>
void Relation::forEachBlockEarlyTermination(const attribute_id column_id,
                                            const Functor &functor) const {
  for (std::size_t block_id = 0; block_id < getNumBlocks(); ++block_id) {
    const StorageBlock &block = getBlock(block_id);
    DCHECK_GT(block.getNumTuples(), static_cast<std::size_t>(0));

    // TODO(robin-team): Handle more block types (e.g. RowStoreBlock).
    DCHECK(block.getStorageBlockType() == StorageBlock::kColumnStore);
    const ColumnStoreBlock &csb = static_cast<const ColumnStoreBlock&>(block);

    const EarlyTerminationStatus status =
        InvokeOnColumnAccessorMakeShared(csb.createColumnAccessor(column_id),
                                         functor);

    if (status == EarlyTerminationStatus::kYes) {
      break;
    }
  }
}

template <typename Functor>
void Relation::forSingletonBlock(const Functor &functor) const {
  CHECK_EQ(1u, getNumBlocks());
  forEachBlock(functor);
}

template <typename Functor>
void Relation::forSingletonBlock(const attribute_id column_id,
                                 const Functor &functor) const {
  CHECK_EQ(1u, getNumBlocks());
  forEachBlock(column_id, functor);
}

}  // namespace project

#endif  // PROJECT_STORAGE_RELATION_HPP_
