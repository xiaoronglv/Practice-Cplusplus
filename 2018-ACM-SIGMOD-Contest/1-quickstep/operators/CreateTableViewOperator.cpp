#include "operators/CreateTableViewOperator.hpp"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>

#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/expressions/ScalarLiteral.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

namespace {

template <typename Comparator, typename Iterator, typename ValueType>
inline Range GetLowerRange(const Iterator &begin,
                           const Iterator &end,
                           const ValueType value) {
  const auto it = std::lower_bound(begin, end, value, Comparator());
  return Range(0, std::distance(begin, it));
}

template <typename Comparator, typename Iterator, typename ValueType>
inline Range GetUpperRange(const Iterator &begin,
                           const Iterator &end,
                           const ValueType value) {
  const auto it = std::upper_bound(begin, end, value, Comparator());
  return Range(std::distance(begin, it), std::distance(begin, end));
}

template <typename Comparator, typename Iterator, typename ValueType>
inline Range GetEqualRange(const Iterator &begin,
                           const Iterator &end,
                           const ValueType value) {
  const auto lo_it = std::lower_bound(begin, end, value, Comparator());
  const auto up_it = std::upper_bound(begin, end, value, Comparator());
  return Range(std::distance(begin, lo_it), std::distance(begin, up_it));
}

template <typename Comparator, typename Iterator, typename ValueType>
inline Range GetRangeForAscendantOrder(const Iterator &begin,
                                       const Iterator &end,
                                       const ValueType value,
                                       const ComparisonType comparison_type) {
  switch (comparison_type) {
    case ComparisonType::kEqual:
      return GetEqualRange<Comparator>(begin, end, value);
    case ComparisonType::kLess:
      return GetLowerRange<Comparator>(begin, end, value);
    case ComparisonType::kGreater:
      return GetUpperRange<Comparator>(begin, end, value);
    default:
      break;
  }
  LOG(FATAL) << "Unsupported comparison in CreateTableViewOperator";
}

template <typename Comparator, typename Iterator, typename ValueType>
inline Range GetRangeForDescendantOrder(const Iterator &begin,
                                        const Iterator &end,
                                        const ValueType value,
                                        const ComparisonType comparison_type) {
  switch (comparison_type) {
    case ComparisonType::kEqual:
      return GetEqualRange<Comparator>(begin, end, value);
    case ComparisonType::kLess:
      return GetUpperRange<Comparator>(begin, end, value);
    case ComparisonType::kGreater:
      return GetLowerRange<Comparator>(begin, end, value);
    default:
      break;
  }
  LOG(FATAL) << "Unsupported comparison in CreateTableViewOperator";
}

}  // namespace

void CreateTableViewOperator::execute(Task *ctx) {
  DCHECK(comparison_->left().getScalarType() == ScalarType::kAttribute);
  DCHECK(comparison_->right().getScalarType() == ScalarType::kLiteral);

  const attribute_id attribute =
      static_cast<const ScalarAttribute&>(comparison_->left()).attribute()->id();

  const std::uint64_t literal =
      static_cast<const ScalarLiteral&>(comparison_->right()).value();

  input_relation_.forEachBlock(
      [&](const auto &block) -> void {
    std::unique_ptr<ColumnAccessor> accessor(block.createColumnAccessor(attribute));

    const Range range = InvokeOnColumnAccessor(accessor.get(),
                                              [&](auto *accessor) -> Range {
      using ValueType = typename std::remove_pointer_t<decltype(accessor)>::ValueType;

      const ValueType *begin = accessor->getData();
      const ValueType *end = begin + accessor->getNumTuples();
      const ValueType value = static_cast<ValueType>(literal);
      const ComparisonType type = this->comparison_->comparison_type();

      switch (this->sort_order_) {
        case SortOrder::kAscendant:
          return GetRangeForAscendantOrder<std::less<ValueType>>(
              begin, end, value, type);
        case SortOrder::kDescendant:
          return GetRangeForDescendantOrder<std::greater<ValueType>>(
              begin, end, value, type);
        default:
          break;
      }
      LOG(FATAL) << "Unsupported SortOrder in CreateTableViewOperator";
    });

    if (range.size() > 0) {
      output_relation_->addBlock(block.createSubset(range));
    }
  });
}

}  // namespace project
