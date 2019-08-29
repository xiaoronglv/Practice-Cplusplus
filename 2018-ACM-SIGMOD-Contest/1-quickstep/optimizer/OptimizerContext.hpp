#ifndef PROJECT_OPTIMIZER_OPTIMIZER_CONTEXT_HPP_
#define PROJECT_OPTIMIZER_OPTIMIZER_CONTEXT_HPP_

#include "optimizer/ExprId.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class OptimizerContext {
 public:
  OptimizerContext() : expr_id_counter_(0) {}

  ExprId nextExprId() {
    return expr_id_counter_++;
  }

 private:
  ExprId expr_id_counter_;

  DISALLOW_COPY_AND_ASSIGN(OptimizerContext);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_OPTIMIZER_CONTEXT_HPP_
