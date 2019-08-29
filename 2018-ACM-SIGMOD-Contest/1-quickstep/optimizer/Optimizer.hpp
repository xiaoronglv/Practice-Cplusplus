#ifndef PROJECT_OPTIMIZER_OPTIMIZER_HPP_
#define PROJECT_OPTIMIZER_OPTIMIZER_HPP_

#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/OptimizerContext.hpp"
#include "optimizer/Plan.hpp"
#include "optimizer/Predicate.hpp"
#include "optimizer/QueryHandle.hpp"
#include "optimizer/Scalar.hpp"
#include "optimizer/TableReference.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class Optimizer {
 public:
  explicit Optimizer(Database *database);

  void generateQueryHandle(const std::string &query,
                           QueryHandle *query_handle);

 private:
  // Optimization passes.
  PlanPtr optimize(const PlanPtr &plan);

  // All-in-one parse and resolve.
  PlanPtr parseQuery(const std::string &query);
  TableReferencePtr parseRelation(const std::string &relation);
  PredicatePtr parsePredicate(const std::string &predicate,
                              const std::vector<TableReferencePtr> &tables);
  ScalarPtr parseScalar(const std::string &scalar,
                        const std::vector<TableReferencePtr> &tables);
  AttributeReferencePtr parseAttribute(const std::string &attribute,
                                       const std::vector<TableReferencePtr> &tables) const;

  Database *database_;
  OptimizerContext optimizer_context_;

  DISALLOW_COPY_AND_ASSIGN(Optimizer);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_OPTIMIZER_HPP_
