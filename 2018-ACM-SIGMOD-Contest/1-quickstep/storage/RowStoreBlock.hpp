#ifndef PROJECT_STORAGE_ROW_STORE_BLOCK_HPP_
#define PROJECT_STORAGE_ROW_STORE_BLOCK_HPP_

#include <cstddef>
#include <cstdlib>
#include <tuple>
#include <vector>

#include "storage/ColumnAccessor.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/meta/Common.hpp"
#include "utility/meta/TypeList.hpp"

#include "glog/logging.h"

namespace project {

class ColumnAccessor;

class RowStoreBlock : public StorageBlock {
 public:
  StorageBlockType getStorageBlockType() const override {
    return kRowStore;
  }

  virtual RowStoreBlock* newInstance(const std::size_t num_tuples) const = 0;

  virtual void copyInto(RowStoreBlock *target, const std::size_t pos) const = 0;

  virtual const void* raw() const = 0;

 protected:
  RowStoreBlock() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(RowStoreBlock);
};


template <typename ...ColumnTypes>
class RowStoreBlockImpl : public RowStoreBlock {
 public:
  using RowStoreBlockType = RowStoreBlockImpl<ColumnTypes...>;
  using Row = std::tuple<ColumnTypes...>;
  using ColumnTypeList = meta::TypeList<ColumnTypes...>;

  explicit RowStoreBlockImpl(const std::size_t num_tuples)
      : num_tuples_(num_tuples),
        data_(static_cast<Row*>(std::malloc(sizeof(Row) * num_tuples))),
        owns_(true) {
    initializeColumnOffsets();
  }

  RowStoreBlockImpl(const RowStoreBlockType &other,
                    const Range &range)
      : num_tuples_(range.size()),
        data_(other.data_ + range.begin()),
        owns_(false) {
    DCHECK_LE(range.end(), other.num_tuples_);
    initializeColumnOffsets();
  }

  ~RowStoreBlockImpl() override {
    if (owns_) {
      std::free(data_);
    }
  }

  const void* raw() const override {
    return data_;
  }

  const Row* data() const {
    return data_;
  }

  Row* dataMutable() {
    return data_;
  }

  RowStoreBlock* newInstance(const std::size_t num_tuples) const override {
    return new RowStoreBlockType(num_tuples);
  }

  void copyInto(RowStoreBlock *target, const std::size_t pos) const override {
    auto *dst = static_cast<RowStoreBlockType*>(target)->data_ + pos;
    std::memcpy(dst, data_, sizeof(Row) * num_tuples_);
  }

  std::size_t getNumColumns() const override {
    return sizeof...(ColumnTypes);
  }

  std::size_t getNumTuples() const override {
    return num_tuples_;
  }

  StorageBlock* createSubset(const Range &range) const override {
    return new RowStoreBlockType(*this, range);
  }

  ColumnAccessor* createColumnAccessor(const attribute_id id) const override {
    return new RowStoreColumnAccessor<char>(
        num_tuples_, sizeof(Row),
        reinterpret_cast<const char*>(data_) + column_offsets_[id]);
  }

  std::uint64_t getValueVirtual(const attribute_id column,
                                const tuple_id tuple) const override {
    LOG(FATAL) << "Not implemented";
  }

  template <std::size_t column, typename T>
  inline void writeValue(const std::size_t pos, const T &value) const {
    std::get<column>(data_[pos]) = value;
  }

  template <std::size_t column>
  inline const auto& at(const std::size_t pos) const {
    return std::get<column>(data_[pos]);
  }

 private:
  void initializeColumnOffsets() {
    meta::MakeSequence<sizeof...(ColumnTypes)>
        ::template bind_to<meta::TypeList>
        ::ForEach([&](auto e) {
      constexpr std::size_t i = decltype(e)::head::value;
      const char *item = reinterpret_cast<const char*>(&std::get<i>(*data_));
      const std::size_t offset = item - reinterpret_cast<const char *>(data_);
      column_offsets_.emplace_back(offset);
    });
  }

  const std::size_t num_tuples_;
  Row *data_;
  const bool owns_;
  std::vector<std::size_t> column_offsets_;

  DISALLOW_COPY_AND_ASSIGN(RowStoreBlockImpl);
};

}  // namespace project

#endif  // PROJECT_STORAGE_ROW_STORE_BLOCK_HPP_
