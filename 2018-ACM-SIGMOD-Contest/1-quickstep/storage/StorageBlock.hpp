#ifndef PROJECT_STORAGE_STORAGE_BLOCK_HPP_
#define PROJECT_STORAGE_STORAGE_BLOCK_HPP_

#include <cstddef>

#include "storage/StorageTypedefs.hpp"
#include "storage/ColumnAccessor.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedArray.hpp"
#include "utility/Range.hpp"

namespace project {

class StorageBlock {
 public:
  enum StorageBlockType {
    kColumnStore = 0,
    kRowStore
  };

  StorageBlock() {}

  virtual ~StorageBlock() {}

  virtual StorageBlockType getStorageBlockType() const = 0;

  virtual std::size_t getNumColumns() const = 0;

  virtual std::size_t getNumTuples() const = 0;

  virtual StorageBlock* createSubset(const Range &range) const = 0;

  virtual ColumnAccessor* createColumnAccessor(const attribute_id id) const = 0;

  virtual std::uint64_t getValueVirtual(const attribute_id column,
                                        const tuple_id tuple) const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(StorageBlock);
};

}  // namespace project

#endif  // PROJECT_STORAGE_STORAGE_BLOCK_HPP_
