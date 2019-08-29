#ifndef PROJECT_OPERATORS_EXPRESSIONS_SCALAR_ATTRIBUTE_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_SCALAR_ATTRIBUTE_HPP_

#include <cstdint>
#include <memory>
#include <vector>

#include "operators/expressions/Scalar.hpp"
#include "storage/Attribute.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class ScalarAttribute : public Scalar {
 public:
  explicit ScalarAttribute(const Attribute *attribute)
      : Scalar(ScalarType::kAttribute),
        attribute_(DCHECK_NOTNULL(attribute)) {}

  const Attribute* attribute() const {
    return attribute_;
  }

  const Type& getResultType() const override {
    return attribute_->getType();
  }

  ColumnVector* getAllValues(const StorageBlock &block,
                             const TupleIdSequence *filter) const override;

  ColumnVector* getAllValues(const StorageBlock &block,
                             const OrderedTupleIdSequence &tuples) const override;

  UInt64Ptr accumulate(const StorageBlock &block,
                       const TupleIdSequence *filter) const override;

  UInt64Ptr accumulateMultiply(
      const StorageBlock &block,
      const OrderedTupleIdSequence &tuples,
      const std::vector<std::uint32_t> &multipliers) const override;

  UInt64Ptr accumulateMultiply(
      const StorageBlock &block,
      const OrderedTupleIdSequence &tuples,
      const std::vector<std::uint64_t> &multipliers) const override;

 private:
  ColumnVector* getAllValues(const StorageBlock &block) const;
  UInt64Ptr accumulate(const StorageBlock &block) const;

  const Attribute *attribute_;

  DISALLOW_COPY_AND_ASSIGN(ScalarAttribute);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_SCALAR_ATTRIBUTE_HPP_
