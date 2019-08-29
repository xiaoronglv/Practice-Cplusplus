#ifndef PROJECT_OPTIMIZER_RULES_ELIMINATE_PREDICATE_LITERAL_HPP_
#define PROJECT_OPTIMIZER_RULES_ELIMINATE_PREDICATE_LITERAL_HPP_

#include <string>

#include "optimizer/Plan.hpp"
#include "optimizer/rules/Rule.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class OptimizerContext;

class EliminatePredicateLiteral : public Rule<Plan> {
 public:
  EliminatePredicateLiteral(OptimizerContext *optimizer_context,
                            Database *database)
      : optimizer_context_(optimizer_context),
        database_(database) {}

  std::string getName() const override {
    return "EliminatePredicateLiteral";
  }

  PlanPtr apply(const PlanPtr &input) override;

 private:
  PlanPtr applyInternal(const PlanPtr &node);

  OptimizerContext *optimizer_context_;
  Database *database_;

  DISALLOW_COPY_AND_ASSIGN(EliminatePredicateLiteral);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_ELIMINATE_PREDICATE_LITERAL_HPP_
