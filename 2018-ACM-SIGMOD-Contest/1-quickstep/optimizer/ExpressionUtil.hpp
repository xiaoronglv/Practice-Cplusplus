#ifndef PROJECT_OPTIMIZER_EXPRESSION_UTIL_HPP_
#define PROJECT_OPTIMIZER_EXPRESSION_UTIL_HPP_

#include <memory>
#include <unordered_set>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/Scalar.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

struct ScalarEqual {
  inline bool operator()(const ScalarPtr &lhs, const ScalarPtr &rhs) const {
    return lhs->getAttribute()->id() == rhs->getAttribute()->id();
  }
};

struct ScalarHash {
  inline std::size_t operator()(const ScalarPtr &scalar) const {
    return std::hash<ExprId>()(scalar->getAttribute()->id());
  }
};

using UnorderedScalarSet =
    std::unordered_set<ScalarPtr, ScalarHash, ScalarEqual>;


std::vector<AttributeReferencePtr> ToRefVector(
    const std::vector<ScalarPtr> &expressions);

std::vector<ExprId> ToExprIdVector(
    const std::vector<AttributeReferencePtr> &attributes);


bool SubsetOfExpressions(const std::vector<ExprId> &left,
                         const std::vector<ExprId> &right);

bool SubsetOfExpressions(const std::vector<AttributeReferencePtr> &left,
                         const std::vector<AttributeReferencePtr> &right);


template <class ExpressionType>
inline bool ContainsExprId(
    const std::vector<std::shared_ptr<const ExpressionType>> &expressions,
    const ExprId expr_id) {
  for (const std::shared_ptr<const ExpressionType> &expression : expressions) {
    if (expression->id() == expr_id) {
      return true;
    }
  }
  return false;
}

std::vector<AttributeReferencePtr> GetReferencedAttributes(
    const std::vector<ScalarPtr> &expressions);

std::vector<ScalarPtr> GetReferencedExpressions(
    const UnorderedAttributeSet &referenced_attributes,
    const std::vector<ScalarPtr> &expressions);


std::vector<PredicatePtr> GetConjunctivePredicates(
    const PredicatePtr &predicate);

PredicatePtr CreateConjunctivePredicate(
    const std::vector<PredicatePtr> &predicates);


void PullUpProjectExpressions(const std::vector<ScalarPtr> &source_expressions,
                              std::vector<ScalarPtr> *target_scalars,
                              PredicatePtr *target_predicate);

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_EXPRESSION_UTIL_HPP_
