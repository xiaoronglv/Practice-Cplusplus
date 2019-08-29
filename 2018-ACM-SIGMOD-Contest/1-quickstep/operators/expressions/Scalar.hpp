#ifndef PROJECT_OPERATORS_EXPRESSIONS_SCALAR_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_SCALAR_HPP_

#include <cstdint>
#include <memory>
#include <vector>

#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace project {

class ColumnVector;
class StorageBlock;

enum class ScalarType {
  kAttribute = 0,
  kLiteral
};

class Scalar {
 public:
  typedef std::shared_ptr<std::uint64_t> UInt64Ptr;

  ScalarType getScalarType() const {
    return scalar_type_;
  }

  virtual const Type& getResultType() const = 0;

  virtual ColumnVector* getAllValues(const StorageBlock &block,
                                     const TupleIdSequence *filter) const = 0;

  virtual ColumnVector* getAllValues(const StorageBlock &block,
                                     const OrderedTupleIdSequence &tuples) const = 0;

  virtual UInt64Ptr accumulate(const StorageBlock &block,
                               const TupleIdSequence *filter) const = 0;

  virtual UInt64Ptr accumulateMultiply(
      const StorageBlock &block,
      const OrderedTupleIdSequence &tuples,
      const std::vector<std::uint32_t> &multipliers) const = 0;

  virtual UInt64Ptr accumulateMultiply(
      const StorageBlock &block,
      const OrderedTupleIdSequence &tuples,
      const std::vector<std::uint64_t> &multipliers) const = 0;

 protected:
  explicit Scalar(const ScalarType scalar_type)
      : scalar_type_(scalar_type) {}

 private:
  const ScalarType scalar_type_;

  DISALLOW_COPY_AND_ASSIGN(Scalar);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_SCALAR_HPP_
