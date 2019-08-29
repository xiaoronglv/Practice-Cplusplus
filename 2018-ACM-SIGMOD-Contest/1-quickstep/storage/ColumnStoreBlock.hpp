#ifndef PROJECT_STORAGE_COLUMN_STORE_BLOCK_HPP_
#define PROJECT_STORAGE_COLUMN_STORE_BLOCK_HPP_

#include <memory>
#include <vector>

#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class ColumnStoreBlock : public StorageBlock {
 public:
  ColumnStoreBlock() : num_tuples_(0) {}

  StorageBlockType getStorageBlockType() const override {
    return kColumnStore;
  }

  std::size_t getNumColumns() const override {
    return columns_.size();
  }

  std::size_t getNumTuples() const override {
    return num_tuples_;
  }

  StorageBlock* createSubset(const Range &range) const override {
    std::unique_ptr<ColumnStoreBlock> csb = std::make_unique<ColumnStoreBlock>();
    for (const auto &column : columns_) {
      csb->addColumn(column->createSubset(range));
    }
    return csb.release();
  }

  ColumnAccessor* createColumnAccessor(const attribute_id id) const override {
    DCHECK_LT(id, columns_.size());
    return columns_[id]->createColumnAccessor();
  }

  std::uint64_t getValueVirtual(const attribute_id column,
                                const tuple_id tuple) const override {
    DCHECK_LT(column, columns_.size());
    return columns_[column]->getValueVirtual(tuple);
  }

  void addColumn(ColumnVector *column) {
    if (columns_.empty()) {
      num_tuples_ = column->getNumTuples();
    }
    DCHECK_EQ(num_tuples_, column->getNumTuples());
    columns_.emplace_back(column);
  }

  void replaceColumn(const attribute_id id, ColumnVector *column) {
    DCHECK_LT(id, columns_.size());
    DCHECK_EQ(num_tuples_, column->getNumTuples());
    columns_[id].reset(column);
  }

 private:
  std::size_t num_tuples_;
  std::vector<std::unique_ptr<ColumnVector>> columns_;

  DISALLOW_COPY_AND_ASSIGN(ColumnStoreBlock);
};

}  // namespace project

#endif  // PROJECT_STORAGE_COLUMN_STORE_BLOCK_HPP_
