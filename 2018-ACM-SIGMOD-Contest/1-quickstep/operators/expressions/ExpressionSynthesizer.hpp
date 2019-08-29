#ifndef PROJECT_OPERATORS_EXPRESSIONS_EXPRESSION_SYNTHESIZER_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_EXPRESSION_SYNTHESIZER_HPP_

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Comparison.hpp"
#include "operators/expressions/Conjunction.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace project {

template <typename Accessor, typename OutputVector>
class AttributeWriter {
 public:
  AttributeWriter(Accessor *accessor, OutputVector *output)
      : accessor_(accessor), output_(output) {}

  inline void operator()(const tuple_id tuple) {
    output_->appendValue(accessor_->at(tuple));
  }

 private:
  const std::unique_ptr<Accessor> accessor_;
  OutputVector * const output_;

  DISALLOW_COPY_AND_ASSIGN(AttributeWriter);
};

template <typename Accessor>
class AttributeSumWriter {
 public:
  AttributeSumWriter(Accessor *accessor, std::uint64_t *output)
      : accessor_(accessor), output_(output) {}

  inline void accumulate(const tuple_id tuple) {
    *output_ += accessor_->at(tuple);
  }

  inline void accumulateMultiply(const tuple_id tuple,
                                 const std::uint64_t multiplier) {
    *output_ += accessor_->at(tuple) * multiplier;
  }

 private:
  const std::unique_ptr<Accessor> accessor_;
  std::uint64_t * const output_;

  DISALLOW_COPY_AND_ASSIGN(AttributeSumWriter);
};

template <std::size_t k, typename Writer>
class CompositeWriter {
 public:
  explicit CompositeWriter(std::vector<std::unique_ptr<Writer>> &&writers)
      : writers_(std::move(writers)) {
    DCHECK_EQ(k, writers_.size());
  }

  inline void accumulate(const tuple_id tuple) {
    for (std::size_t i = 0; i < k; ++i) {
      writers_[i]->accumulate(tuple);
    }
  }

  inline void accumulateMultiply(const tuple_id tuple,
                                 const std::uint64_t multiplier) {
    for (std::size_t i = 0; i < k; ++i) {
      writers_[i]->accumulateMultiply(tuple, multiplier);
    }
  }

 private:
  const std::vector<std::unique_ptr<Writer>> writers_;

  DISALLOW_COPY_AND_ASSIGN(CompositeWriter);
};

template <typename ScalarReference, typename Functor>
bool InvokeOnAttributeSum(
    const std::vector<ScalarReference> &expressions,
    std::vector<std::uint64_t> *sums,
    const StorageBlock &block,
    const Functor &functor) {
  const std::size_t num_attributes = expressions.size();
  if (num_attributes == 0 || num_attributes > 4) {
    return false;
  }

  const TypeID type_id = expressions.front()->getResultType().getTypeID();
  for (const auto &expr : expressions) {
    if (expr->getScalarType() != ScalarType::kAttribute ||
        expr->getResultType().getTypeID() != type_id) {
      return false;
    }
  }

  // TODO(robin-team): Handle more block types (e.g. RowStoreBlock).
  DCHECK(block.getStorageBlockType() == StorageBlock::kColumnStore);
  const ColumnStoreBlock &csb = static_cast<const ColumnStoreBlock&>(block);

  return InvokeOnTypeIDForCppType(
      type_id,
      [&](auto ic) {
    using CppType = typename decltype(ic)::head;
    using Accessor = ColumnStoreColumnAccessor<CppType>;
    using Writer = AttributeSumWriter<Accessor>;

    std::vector<std::unique_ptr<Writer>> writers(num_attributes);
    for (std::size_t i = 0; i < num_attributes; ++i) {
      writers[i] = std::make_unique<Writer>(
          static_cast<Accessor*>(csb.createColumnAccessor(
              static_cast<const ScalarAttribute&>(*expressions[i]).attribute()->id())),
          &sums->at(i));
    }

    switch (num_attributes) {
      case 1:
        return functor(
            std::make_shared<CompositeWriter<1, Writer>>(std::move(writers)));
      case 2:
        return functor(
            std::make_shared<CompositeWriter<2, Writer>>(std::move(writers)));
        break;
      case 3:
        return functor(
            std::make_shared<CompositeWriter<3, Writer>>(std::move(writers)));
      case 4:
        return functor(
            std::make_shared<CompositeWriter<4, Writer>>(std::move(writers)));
      default:
       LOG(FATAL) << "Unexpected number of attributes in InvokeOnAttributeSum()";
    }
  });
}

template <typename Functor>
bool InvokeOnConjunctionPredicate(const Conjunction &conjunction,
                                  const StorageBlock &block,
                                  const Functor &functor) {
  // Specialize on conjunction of two predicates, where the first predicate is
  // an equal-comparison between two attributes, and the second predicate is a
  // comparison between one attribute and one literal.
  const auto &operands = conjunction.operands();
  if (operands.size() != 2) {
    return false;
  }
  if (operands[0]->getPredicateType() != PredicateType::kComparison ||
      operands[1]->getPredicateType() != PredicateType::kComparison) {
    return false;
  }

  const Comparison &first = static_cast<const Comparison&>(*operands[0]);
  if (first.comparison_type() != ComparisonType::kEqual ||
      first.left().getScalarType() != ScalarType::kAttribute ||
      first.right().getScalarType() != ScalarType::kAttribute) {
    return false;
  }

  const Comparison &second = static_cast<const Comparison&>(*operands[1]);
  if (second.left().getScalarType() != ScalarType::kAttribute ||
      second.right().getScalarType() != ScalarType::kLiteral) {
    return false;
  }

  return first.invokeOnAttributeEqualComparison(
      block, [&](auto first_comparator) -> bool {
    return second.invokeOnLiteralComparison(
        block, [&](auto second_comparator) -> bool {
      using FirstComparator =
          std::remove_pointer_t<decltype(first_comparator.get())>;
      using SecondComparator =
          std::remove_pointer_t<decltype(second_comparator.get())>;
      using ConjunctionFunctor =
          TwoPredicateConjunctionFunctor<FirstComparator, SecondComparator>;

      std::shared_ptr<ConjunctionFunctor> conjunction =
          std::make_shared<ConjunctionFunctor>(first_comparator,
                                               second_comparator);
      return functor(conjunction);
    });
  });
}

template <typename Functor>
bool InvokeOnLiteralComparisonPredicate(const Predicate *predicate,
                                        const StorageBlock &block,
                                        const Functor &functor) {
  if (predicate == nullptr ||
      predicate->getPredicateType() != PredicateType::kComparison) {
    return false;
  }

  const Comparison &comparison = static_cast<const Comparison&>(*predicate);
  if (comparison.left().getScalarType() != ScalarType::kAttribute ||
      comparison.right().getScalarType() != ScalarType::kLiteral) {
    return false;
  }

  return comparison.invokeOnLiteralComparison(block, functor);
}

// Incurs long compile time ...
template <typename Functor>
bool InvokeOnPredicate(const Predicate *predicate,
                       const StorageBlock &block,
                       const Functor &functor) {
  if (predicate == nullptr) {
    return false;
  }

  switch (predicate->getPredicateType()) {
    case PredicateType::kComparison:
      return static_cast<const Comparison&>(*predicate)
          .invokeOnAnyComparison(block, functor);
    case PredicateType::kConjunction:
      return InvokeOnConjunctionPredicate(
          static_cast<const Conjunction&>(*predicate), block, functor);
    default:
      break;
  }
  return false;
}

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_EXPRESSION_SYNTHESIZER_HPP_

