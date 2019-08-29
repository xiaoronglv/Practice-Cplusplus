#ifndef PROJECT_OPTIMIZER_PATTERN_MATCHER_HPP_
#define PROJECT_OPTIMIZER_PATTERN_MATCHER_HPP_

#include <memory>
#include <type_traits>

#include "optimizer/Plan.hpp"
#include "optimizer/Expression.hpp"

namespace project {
namespace optimizer {

class Aggregate;
class AggregateFunction;
class ChainedJoinAggregate;
class Comparison;
class EquiJoin;
class EquiJoinAggregate;
class Filter;
class MultiwayCartesianJoin;
class Project;
class Selection;
class TableReference;
class TableView;
class TopLevelPlan;

class Alias;
class AttributeReference;
class Comparison;
class Conjunction;
class Predicate;
class PredicateLiteral;
class Scalar;
class ScalarLiteral;


template <class PlanClass, PlanType ...plan_types>
class SomePlanNode {
 public:
  template <class OtherPlanClass>
  static bool Matches(const std::shared_ptr<const OtherPlanClass> &plan) {
    for (const PlanType plan_type : kPlanTypes) {
      if (plan->getPlanType() == plan_type) {
        return true;
      }
    }
    return false;
  }

  template <class OtherPlanClass>
  static bool MatchesWithConditionalCast(
      const std::shared_ptr<const OtherPlanClass> &plan,
      std::shared_ptr<const PlanClass> *cast_plan) {
    bool is_match = Matches(plan);
    if (is_match) {
      *cast_plan = std::static_pointer_cast<const PlanClass>(plan);
    }
    return is_match;
  }

 private:
  constexpr static PlanType kPlanTypes[] = { plan_types... };
};

template <class PlanClass, PlanType ...plan_types>
constexpr PlanType SomePlanNode<PlanClass, plan_types...>::kPlanTypes[];


template <class ExpressionClass, ExpressionType ...expression_types>
class SomeExpressionNode {
 public:
  template <class OtherExpressionClass>
  static bool Matches(const std::shared_ptr<const OtherExpressionClass> &expression) {
    for (const ExpressionType expression_type : kExpressionTypes) {
      if (expression->getExpressionType() == expression_type) {
        return true;
      }
    }
    return false;
  }

  template <class OtherExpressionClass>
  static bool MatchesWithConditionalCast(
      const std::shared_ptr<const OtherExpressionClass> &expression,
      std::shared_ptr<const ExpressionClass> *cast_expression) {
    bool is_match = Matches(expression);
    if (is_match) {
      *cast_expression = std::static_pointer_cast<const ExpressionClass>(expression);
    }
    return is_match;
  }

 private:
  constexpr static ExpressionType kExpressionTypes[] = { expression_types... };
};

template <class ExpressionClass, ExpressionType ...expression_types>
constexpr ExpressionType SomeExpressionNode<
    ExpressionClass, expression_types...>::kExpressionTypes[];


using SomeAggregate = SomePlanNode<Aggregate, PlanType::kAggregate>;
using SomeChainedJoinAggregate = SomePlanNode<ChainedJoinAggregate,
                                              PlanType::kChainedJoinAggregate>;
using SomeEquiJoin = SomePlanNode<EquiJoin, PlanType::kEquiJoin>;
using SomeEquiJoinAggregate = SomePlanNode<EquiJoinAggregate,
                                           PlanType::kEquiJoinAggregate>;
using SomeFilter = SomePlanNode<Filter, PlanType::kFilter>;
using SomeMultiwayCartesianJoin = SomePlanNode<MultiwayCartesianJoin,
                                               PlanType::kMultiwayCartesianJoin>;
using SomeProject = SomePlanNode<Project, PlanType::kProject>;
using SomeSelection = SomePlanNode<Selection, PlanType::kSelection>;
using SomeTableReference = SomePlanNode<TableReference, PlanType::kTableReference>;
using SomeTableView = SomePlanNode<TableView, PlanType::kTableView>;
using SomeTopLevelPlan = SomePlanNode<TopLevelPlan, PlanType::kTopLevelPlan>;


using SomeAggregateFunction = SomeExpressionNode<AggregateFunction,
                                                 ExpressionType::kAggregateFunction>;
using SomeAlias = SomeExpressionNode<Alias, ExpressionType::kAlias>;
using SomeAttributeReference = SomeExpressionNode<AttributeReference,
                                                  ExpressionType::kAttributeReference>;
using SomeComparison = SomeExpressionNode<Comparison, ExpressionType::kComparison>;
using SomeConjunction = SomeExpressionNode<Conjunction, ExpressionType::kConjunction>;
using SomePredicate = SomeExpressionNode<Predicate, ExpressionType::kComparison,
                                                    ExpressionType::kConjunction,
                                                    ExpressionType::kPredicateLiteral>;
using SomePredicateLiteral = SomeExpressionNode<PredicateLiteral,
                                                ExpressionType::kPredicateLiteral>;
using SomeScalar = SomeExpressionNode<Scalar, ExpressionType::kAlias,
                                              ExpressionType::kAttributeReference,
                                              ExpressionType::kScalarLiteral>;
using SomeScalarLiteral = SomeExpressionNode<ScalarLiteral,
                                             ExpressionType::kScalarLiteral>;

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_PATTERN_MATCHER_HPP_
