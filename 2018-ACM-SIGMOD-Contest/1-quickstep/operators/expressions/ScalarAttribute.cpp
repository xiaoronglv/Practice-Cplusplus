#include "operators/expressions/ScalarAttribute.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

#include "storage/ColumnVector.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"

namespace project {

ColumnVector* ScalarAttribute::getAllValues(
    const StorageBlock &block, const TupleIdSequence *filter) const {
  if (filter == nullptr) {
    return getAllValues(block);
  }

  return InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    using Accessor = std::remove_pointer_t<decltype(accessor.get())>;
    using CppType = typename Accessor::ValueType;
    using OutputColumnVector = ColumnVectorImpl<CppType>;

    std::unique_ptr<OutputColumnVector> output_column =
        std::make_unique<OutputColumnVector>(filter->getNumTuples());

    for (const tuple_id tuple : *filter) {
      output_column->appendValue(accessor->at(tuple));
    }

    return static_cast<ColumnVector*>(output_column.release());
  });
}

ColumnVector* ScalarAttribute::getAllValues(
    const StorageBlock &block, const OrderedTupleIdSequence &tuples) const {
  return InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    using Accessor = std::remove_pointer_t<decltype(accessor.get())>;
    using CppType = typename Accessor::ValueType;
    using OutputColumnVector = ColumnVectorImpl<CppType>;

    std::unique_ptr<OutputColumnVector> output_column =
        std::make_unique<OutputColumnVector>(tuples.size());

    for (const tuple_id tuple : tuples) {
      output_column->appendValue(accessor->at(tuple));
    }

    return static_cast<ColumnVector*>(output_column.release());
  });
}

ScalarAttribute::UInt64Ptr ScalarAttribute::accumulate(
    const StorageBlock &block, const TupleIdSequence *filter) const {
  if (filter == nullptr) {
    return accumulate(block);
  }

  if (filter->begin() == filter->end()) {
    return nullptr;
  }

  std::uint64_t sum = 0;
  InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    for (const tuple_id tuple : *filter) {
      sum += accessor->at(tuple);
    }
  });
  return std::make_shared<std::uint64_t>(sum);
}

ColumnVector* ScalarAttribute::getAllValues(const StorageBlock &block) const {
  return InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    using Accessor = std::remove_pointer_t<decltype(accessor.get())>;
    using CppType = typename Accessor::ValueType;
    using OutputColumnVector = ColumnVectorImpl<CppType>;

    const std::size_t num_tuples = accessor->getNumTuples();
    std::unique_ptr<OutputColumnVector> output_column =
        std::make_unique<OutputColumnVector>(num_tuples);

    for (std::size_t i = 0; i < num_tuples; ++i) {
      output_column->appendValue(accessor->at(i));
    }

    return static_cast<ColumnVector*>(output_column.release());
  });
}

ScalarAttribute::UInt64Ptr ScalarAttribute::accumulate(
    const StorageBlock &block) const {
  const std::size_t num_tuples = block.getNumTuples();
  if (num_tuples == 0) {
    return nullptr;
  }

  std::uint64_t sum = 0;
  InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    for (std::size_t i = 0; i < accessor->getNumTuples(); ++i) {
      sum += accessor->at(i);
    }
  });
  return std::make_shared<std::uint64_t>(sum);
}

ScalarAttribute::UInt64Ptr ScalarAttribute::accumulateMultiply(
    const StorageBlock &block,
    const OrderedTupleIdSequence &tuples,
    const std::vector<std::uint32_t> &multipliers) const {
  const std::size_t num_tuples = tuples.size();
  if (num_tuples == 0) {
    return nullptr;
  }

  DCHECK_EQ(num_tuples, multipliers.size());
  std::uint64_t sum = 0;
  InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    for (std::size_t i = 0; i < num_tuples; ++i) {
      const std::uint64_t multiplier = multipliers[i];
      sum += accessor->at(tuples[i]) * multiplier;
    }
  });
  return std::make_shared<std::uint64_t>(sum);
}

ScalarAttribute::UInt64Ptr ScalarAttribute::accumulateMultiply(
    const StorageBlock &block,
    const OrderedTupleIdSequence &tuples,
    const std::vector<std::uint64_t> &multipliers) const {
  const std::size_t num_tuples = tuples.size();
  if (num_tuples == 0) {
    return nullptr;
  }

  DCHECK_EQ(num_tuples, multipliers.size());
  std::uint64_t sum = 0;
  InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(attribute_->id()),
      [&](auto accessor) {
    for (std::size_t i = 0; i < num_tuples; ++i) {
      sum += accessor->at(tuples[i]) * multipliers[i];
    }
  });
  return std::make_shared<std::uint64_t>(sum);
}

}  // namespace project
