#ifndef PROJECT_OPERATORS_EXPRESSIONS_COMPARISON_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_COMPARISON_HPP_

#include <cstdint>
#include <memory>

#include "operators/expressions/ComparisonType.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/expressions/ScalarLiteral.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class TupleIdSequence;

class Comparison : public Predicate {
 public:
  Comparison(const ComparisonType comparison_type,
             Scalar *left,
             Scalar *right)
      : Predicate(PredicateType::kComparison),
        comparison_type_(comparison_type),
        left_(left),
        right_(right) {
  }

  TupleIdSequence* getAllMatches(const StorageBlock &block,
                                 TupleIdSequence *filter) const override;

  Range reduceRange(const Range &range, bool *exactness) const override;

  bool impliesExactRange(const attribute_id id) const override;

  ComparisonType comparison_type() const {
    return comparison_type_;
  }

  const Scalar& left() const {
    return *left_;
  }

  const Scalar& right() const {
    return *right_;
  }

  template <typename Functor>
  inline auto invokeOnAnyComparison(const StorageBlock &block,
                                    const Functor &functor) const;

  template <typename Functor>
  inline auto invokeOnLiteralComparison(const StorageBlock &block,
                                        const Functor &functor) const;

  template <typename Functor>
  inline auto invokeOnAttributeEqualComparison(const StorageBlock &block,
                                               const Functor &functor) const;

 private:
  const ComparisonType comparison_type_;
  const std::unique_ptr<Scalar> left_;
  const std::unique_ptr<Scalar> right_;

  DISALLOW_COPY_AND_ASSIGN(Comparison);
};

// ----------------------------------------------------------------------------
// Comparison functors

template <ComparisonType comparison_type>
struct ComparisonTypeTrait;

template <>
struct ComparisonTypeTrait<ComparisonType::kEqual> {
  template <typename LeftCppType, typename RightCppType>
  inline static bool Compare(const LeftCppType left, const RightCppType right) {
    return left == right;
  }
};

template <>
struct ComparisonTypeTrait<ComparisonType::kLess> {
  template <typename LeftCppType, typename RightCppType>
  inline static bool Compare(const LeftCppType left, const RightCppType right) {
    return left < right;
  }
};

template <>
struct ComparisonTypeTrait<ComparisonType::kGreater> {
  template <typename LeftCppType, typename RightCppType>
  inline static bool Compare(const LeftCppType left, const RightCppType right) {
    return left > right;
  }
};

template <typename Functor>
inline auto InvokeOnComparisonType(const ComparisonType comparison_type,
                                   const Functor &functor) {
  switch (comparison_type) {
    case ComparisonType::kEqual:
      return functor(std::integral_constant<ComparisonType,
                                            ComparisonType::kEqual>());
    case ComparisonType::kLess:
      return functor(std::integral_constant<ComparisonType,
                                            ComparisonType::kLess>());
    case ComparisonType::kGreater:
      return functor(std::integral_constant<ComparisonType,
                                            ComparisonType::kGreater>());
    default:
      break;
  }
  LOG(FATAL) << "Unexpected ComparisonType at InvokeOnComparisonType()";
}

template <typename LeftCppType, typename RightCppType,
          typename LeftAccessor, typename RightAccessor>
class AttributeToAttributeEqualComparator {
 public:
  AttributeToAttributeEqualComparator(LeftAccessor *left_accessor,
                                      RightAccessor *right_accessor)
      : left_accessor_(left_accessor),
        right_accessor_(right_accessor) {}

  inline bool operator()(const tuple_id tuple) const {
    return left_accessor_->at(tuple) == right_accessor_->at(tuple);
  }

 private:
  const std::unique_ptr<LeftAccessor> left_accessor_;
  const std::unique_ptr<RightAccessor> right_accessor_;

  DISALLOW_COPY_AND_ASSIGN(AttributeToAttributeEqualComparator);
};

template <typename CppType, typename Accessor, ComparisonType comparison_type>
class AttributeToLiteralComparator {
 private:
  using Trait = ComparisonTypeTrait<comparison_type>;

 public:
  AttributeToLiteralComparator(Accessor *accessor, const CppType literal)
      : accessor_(accessor), literal_(literal) {}

  inline bool operator()(const tuple_id tuple) const {
    return Trait::Compare(accessor_->at(tuple), literal_);
  }

 private:
  const std::unique_ptr<Accessor> accessor_;
  const CppType literal_;

  DISALLOW_COPY_AND_ASSIGN(AttributeToLiteralComparator);
};

template <typename Functor>
inline auto Comparison::invokeOnAnyComparison(
    const StorageBlock &block, const Functor &functor) const {
  if (right_->getScalarType() == ScalarType::kAttribute) {
    return invokeOnAttributeEqualComparison(block, functor);
  } else {
    return invokeOnLiteralComparison(block, functor);
  }
}
template <typename Functor>
inline auto Comparison::invokeOnAttributeEqualComparison(
    const StorageBlock &block, const Functor &functor) const {
  DCHECK(comparison_type_ == ComparisonType::kEqual);
  DCHECK(left_->getScalarType() == ScalarType::kAttribute);
  DCHECK(right_->getScalarType() == ScalarType::kAttribute);

  // TODO(robin-team): Handle more block types (e.g. RowStoreBlock).
  DCHECK(block.getStorageBlockType() == StorageBlock::kColumnStore);
  const ColumnStoreBlock &csb = static_cast<const ColumnStoreBlock&>(block);

  const ScalarAttribute *left_attr =
      static_cast<const ScalarAttribute*>(left_.get());
  const attribute_id left_attr_id = left_attr->attribute()->id();

  const ScalarAttribute *right_attr =
      static_cast<const ScalarAttribute*>(right_.get());
  const attribute_id right_attr_id = right_attr->attribute()->id();

  return InvokeOnTypeIDsForCppTypes(
      left_attr->getResultType().getTypeID(),
      right_attr->getResultType().getTypeID(),
      [&](auto typelist) {
    using LeftCppType = typename decltype(typelist)::template at<0>;
    using RightCppType = typename decltype(typelist)::template at<1>;
    using LeftAccessor = ColumnStoreColumnAccessor<LeftCppType>;
    using RightAccessor = ColumnStoreColumnAccessor<RightCppType>;

    using Comparator = AttributeToAttributeEqualComparator<
        LeftCppType, RightCppType, LeftAccessor, RightAccessor>;

    std::shared_ptr<Comparator> comparator = std::make_shared<Comparator>(
        ColumnAccessor::Cast<LeftAccessor>(csb.createColumnAccessor(left_attr_id)),
        ColumnAccessor::Cast<RightAccessor>(csb.createColumnAccessor(right_attr_id)));

    return functor(comparator);
  });
}

template <typename Functor>
inline auto Comparison::invokeOnLiteralComparison(
    const StorageBlock &block, const Functor &functor) const {
  DCHECK(left_->getScalarType() == ScalarType::kAttribute);
  DCHECK(right_->getScalarType() == ScalarType::kLiteral);

  // TODO(robin-team): Handle more block types (e.g. RowStoreBlock).
  DCHECK(block.getStorageBlockType() == StorageBlock::kColumnStore);
  const ColumnStoreBlock &csb = static_cast<const ColumnStoreBlock&>(block);

  const ScalarAttribute *left_attr =
      static_cast<const ScalarAttribute*>(left_.get());
  const attribute_id left_attr_id = left_attr->attribute()->id();

  const ScalarLiteral *right_literal =
      static_cast<const ScalarLiteral*>(right_.get());

  return InvokeOnTypeIDForCppType(
      left_attr->getResultType().getTypeID(),
      [&](auto tic) {
    using CppType = typename decltype(tic)::head;
    using Accessor = ColumnStoreColumnAccessor<CppType>;

    return InvokeOnComparisonType(
        comparison_type_,
        [&](auto cic) {
      constexpr ComparisonType kType = decltype(cic)::value;
      using Comparator = AttributeToLiteralComparator<CppType, Accessor, kType>;

      std::shared_ptr<Comparator> comparator = std::make_shared<Comparator>(
          ColumnAccessor::Cast<Accessor>(csb.createColumnAccessor(left_attr_id)),
          static_cast<CppType>(right_literal->value()));

      return functor(comparator);
    });
  });
}

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_COMPARISON_HPP_
