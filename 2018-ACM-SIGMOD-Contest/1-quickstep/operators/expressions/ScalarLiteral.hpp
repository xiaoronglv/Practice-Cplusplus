#ifndef PROJECT_OPERATORS_EXPRESSIONS_SCALAR_LITERAL_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_SCALAR_LITERAL_HPP_

#include <cstdint>
#include <memory>
#include <vector>

#include "operators/expressions/Scalar.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class ScalarLiteral : public Scalar {
 public:
  ScalarLiteral(const std::uint64_t value,
                const Type &value_type)
      : Scalar(ScalarType::kLiteral),
        value_(value),
        value_type_(value_type) {}

  std::uint64_t value() const {
    return value_;
  }

  const Type& getResultType() const override {
    return value_type_;
  }

  ColumnVector* getAllValues(const StorageBlock &block,
                             const TupleIdSequence *filter) const override {
    LOG(FATAL) << "Not implemented";
  }

  ColumnVector* getAllValues(const StorageBlock &block,
                             const OrderedTupleIdSequence &tuples) const override {
    LOG(FATAL) << "Not implemented";
  }

  UInt64Ptr accumulate(const StorageBlock &block,
                       const TupleIdSequence *filter) const override {
    LOG(FATAL) << "Not implemented";
  }

  UInt64Ptr accumulateMultiply(
      const StorageBlock &block,
      const OrderedTupleIdSequence &tuples,
      const std::vector<std::uint32_t> &multipliers) const override {
    LOG(FATAL) << "Not implemented";
  }

  UInt64Ptr accumulateMultiply(
      const StorageBlock &block,
      const OrderedTupleIdSequence &tuples,
      const std::vector<std::uint64_t> &multipliers) const override {
    LOG(FATAL) << "Not implemented";
  }

 private:
  const std::uint64_t value_;
  const Type &value_type_;

  DISALLOW_COPY_AND_ASSIGN(ScalarLiteral);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_SCALAR_LITERAL_HPP_
