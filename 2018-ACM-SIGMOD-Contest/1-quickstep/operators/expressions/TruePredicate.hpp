#ifndef PROJECT_OPERATORS_EXPRESSIONS_TRUE_PREDICATE_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_TRUE_PREDICATE_HPP_

#include "operators/expressions/Predicate.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {

class TupleIdSequence;
class StorageBlock;

class TruePredicate : public Predicate {
 public:
  TruePredicate()
      : Predicate(PredicateType::kTrue) {
  }

  TupleIdSequence* getAllMatches(const StorageBlock &block,
                                 TupleIdSequence *filter) const override;

  Range reduceRange(const Range &range, bool *exactness) const override;

  bool impliesExactRange(const attribute_id id) const override;

 private:
  DISALLOW_COPY_AND_ASSIGN(TruePredicate);
};

// ----------------------------------------------------------------------------
// "Always true" functor

class TrueFunctor {
 public:
  TrueFunctor() {}

  inline bool operator()(const tuple_id tuple) const {
    return true;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(TrueFunctor);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_TRUE_PREDICATE_HPP_
