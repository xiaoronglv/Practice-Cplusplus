#ifndef PROJECT_OPERATORS_EXPRESSIONS_CONJUNCTION_HPP_
#define PROJECT_OPERATORS_EXPRESSIONS_CONJUNCTION_HPP_

#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Predicate.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class TupleIdSequence;

class Conjunction : public Predicate {
 public:
  explicit Conjunction(std::vector<std::unique_ptr<Predicate>> &&operands)
      : Predicate(PredicateType::kConjunction),
        operands_(std::move(operands)) {
    DCHECK(!operands_.empty());
  }

  TupleIdSequence* getAllMatches(const StorageBlock &block,
                                 TupleIdSequence *filter) const override;

  Range reduceRange(const Range &range, bool *exactness) const override;

  bool impliesExactRange(const attribute_id id) const override;

  const std::vector<std::unique_ptr<Predicate>>& operands() const {
    return operands_;
  }

 private:
  const std::vector<std::unique_ptr<Predicate>> operands_;

  DISALLOW_COPY_AND_ASSIGN(Conjunction);
};

// ----------------------------------------------------------------------------
// Conjunction functors

template <typename FirstPredicate, typename SecondPredicate>
class TwoPredicateConjunctionFunctor {
 public:
  TwoPredicateConjunctionFunctor(
      const std::shared_ptr<FirstPredicate> &first_predicate,
      const std::shared_ptr<SecondPredicate> &second_predicate)
      : first_predicate_(first_predicate),
        second_predicate_(second_predicate) {}

  inline bool operator()(const tuple_id tuple) const {
    return (*first_predicate_)(tuple) && (*second_predicate_)(tuple);
  }

 private:
  const std::shared_ptr<FirstPredicate> first_predicate_;
  const std::shared_ptr<SecondPredicate> second_predicate_;

  DISALLOW_COPY_AND_ASSIGN(TwoPredicateConjunctionFunctor);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_EXPRESSIONS_CONJUNCTION_HPP_
