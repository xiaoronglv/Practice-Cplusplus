#ifndef PROJECT_OPERATORS_EXPRESSIONS_PREDICATE_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_PREDICATE_HPP_

#include <cstdint>

#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class StorageBlock;
class TupleIdSequence;

enum class PredicateType {
  kComparison = 0,
  kConjunction,
  kTrue
};

class Predicate {
 public:
  PredicateType getPredicateType() const {
    return predicate_type_;
  }

  virtual TupleIdSequence* getAllMatches(const StorageBlock &block,
                                         TupleIdSequence *filter) const = 0;

  virtual Range reduceRange(const Range &range, bool *exactness) const = 0;

  virtual bool impliesExactRange(const attribute_id id) const = 0;

 protected:
  explicit Predicate(const PredicateType predicate_type)
      : predicate_type_(predicate_type) {}

 private:
  const PredicateType predicate_type_;

  DISALLOW_COPY_AND_ASSIGN(Predicate);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_PREDICATE_HPP_
