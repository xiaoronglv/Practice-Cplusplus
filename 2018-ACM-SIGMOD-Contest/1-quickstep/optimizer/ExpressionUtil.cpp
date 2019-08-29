#include "optimizer/ExpressionUtil.hpp"

#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Conjunction.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Expression.hpp"
#include "optimizer/PatternMatcher.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/PredicateLiteral.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/rules/UpdateExpression.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/MemoryUtil.hpp"

namespace project {
namespace optimizer {

namespace {

void PullUpProjectExpressions(
    const std::vector<ScalarPtr> &source_expressions,
    std::vector<std::vector<ExpressionPtr>*> *target_expressions) {
  std::unordered_map<ExprId, ExpressionPtr> substitution_map;
  for (const ScalarPtr &scalar : source_expressions) {
    if (scalar->getExpressionType() != ExpressionType::kAttributeReference) {
      substitution_map.emplace(scalar->getAttribute()->id(), scalar);
    }
  }

  if (substitution_map.empty()) {
    return;
  }

  UpdateExpression rule(substitution_map);
  for (std::vector<ExpressionPtr> *expressions : *target_expressions) {
    for (ExpressionPtr &expression : *expressions) {
      expression = rule.apply(expression);
    }
  }
}

}  // namespace

std::vector<AttributeReferencePtr> ToRefVector(
    const std::vector<ScalarPtr> &expressions) {
  std::vector<AttributeReferencePtr> cast_expressions;
  for (const ScalarPtr &scalar : expressions) {
    cast_expressions.emplace_back(scalar->getAttribute());
  }
  return cast_expressions;
}

std::vector<ExprId> ToExprIdVector(
    const std::vector<AttributeReferencePtr> &attributes) {
  std::vector<ExprId> expr_id_vector;
  for (const auto &attribute : attributes) {
    expr_id_vector.emplace_back(attribute->id());
  }
  return expr_id_vector;
}

bool SubsetOfExpressions(const std::vector<ExprId> &left,
                         const std::vector<ExprId> &right) {
  std::unordered_set<ExprId> supset(right.begin(), right.end());
  for (const ExprId expr_id : left) {
    if (supset.find(expr_id) == supset.end()) {
      return false;
    }
  }
  return true;
}

bool SubsetOfExpressions(const std::vector<AttributeReferencePtr> &left,
                         const std::vector<AttributeReferencePtr> &right) {
  std::unordered_set<ExprId> supset;
  for (const auto &attr : right) {
    supset.emplace(attr->id());
  }
  for (const auto &attr : left) {
    if (supset.find(attr->id()) == supset.end()) {
      return false;
    }
  }
  return true;
}

std::vector<AttributeReferencePtr> GetReferencedAttributes(
    const std::vector<ScalarPtr> &expressions) {
  std::vector<AttributeReferencePtr> referenced_attributes;
  for (const ScalarPtr &scalar : expressions) {
    InsertAll(scalar->getReferencedAttributes(), &referenced_attributes);
  }
  return referenced_attributes;
}

std::vector<ScalarPtr> GetReferencedExpressions(
    const UnorderedAttributeSet &referenced_attributes,
    const std::vector<ScalarPtr> &expressions) {
  UnorderedScalarSet referenced_expressions;
  for (const auto &expression : expressions) {
    if (referenced_attributes.find(expression->getAttribute())
            != referenced_attributes.end()) {
      referenced_expressions.emplace(expression);
    }
  }
  return std::vector<ScalarPtr>(referenced_expressions.begin(),
                                referenced_expressions.end());
}

std::vector<PredicatePtr> GetConjunctivePredicates(
    const PredicatePtr &predicate) {
  ConjunctionPtr conjunction;
  if (SomeConjunction::MatchesWithConditionalCast(predicate, &conjunction)) {
    std::vector<PredicatePtr> predicates;
    for (const auto &predicate : conjunction->operands()) {
      InsertAll(GetConjunctivePredicates(predicate), &predicates);
    }
    return predicates;
  } else {
    return { predicate };
  }
}

PredicatePtr CreateConjunctivePredicate(
    const std::vector<PredicatePtr> &predicates) {
  std::vector<PredicatePtr> flattened;
  for (const auto &predicate : predicates) {
    InsertAll(GetConjunctivePredicates(predicate), &flattened);
  }

  if (flattened.empty()) {
    return PredicateLiteral::Create(true);
  } else if (flattened.size() == 1) {
    return flattened.front();
  }
  return Conjunction::Create(flattened);
}

void PullUpProjectExpressions(const std::vector<ScalarPtr> &source_expressions,
                              std::vector<ScalarPtr> *target_scalars,
                              PredicatePtr *target_predicate) {
  const bool has_scalars =
      target_scalars != nullptr;
  const bool has_predicate =
      target_predicate != nullptr && *target_predicate != nullptr;

  if (!has_scalars && !has_predicate) {
    return;
  }

  std::vector<std::vector<ExpressionPtr>*> target_expressions;
  std::vector<ExpressionPtr> scalar_expressions, predicate_expressions;
  if (has_scalars) {
    scalar_expressions = CastSharedPtrVector<Expression>(*target_scalars);
    target_expressions.emplace_back(&scalar_expressions);
  }
  if (has_predicate) {
    predicate_expressions.emplace_back(*target_predicate);
    target_expressions.emplace_back(&predicate_expressions);
  }

  PullUpProjectExpressions(source_expressions, &target_expressions);

  std::size_t idx = 0;
  if (has_scalars) {
    *target_scalars = CastSharedPtrVector<Scalar>(*target_expressions.at(idx++));
  }
  if (has_predicate) {
    *target_predicate =
        std::static_pointer_cast<const Predicate>(target_expressions.at(idx)->front());
  }
}

}  // namespace optimizer
}  // namespace project
