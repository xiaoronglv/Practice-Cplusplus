#ifndef PROJECT_OPERATORS_RELATIONAL_OPERATOR_HPP_
#define PROJECT_OPERATORS_RELATIONAL_OPERATOR_HPP_

#include <cstddef>
#include <string>

#include "scheduler/Task.hpp"
#include "scheduler/TaskDescription.hpp"
#include "utility/Macros.hpp"

namespace project {

class RelationalOperator {
 public:
  virtual std::string getName() const = 0;

  std::size_t getOperatorIndex() const {
    return op_index_;
  }

  void setOperatorIndex(const std::size_t op_index) {
    op_index_ = op_index;
  }

  std::size_t getQueryId() const {
    return query_id_;
  }

  virtual void execute(Task *ctx) = 0;

 protected:
  explicit RelationalOperator(const std::size_t query_id)
      : query_id_(query_id) {}

  const std::size_t query_id_;

 private:
  Task* createTask() {
    return CreateLambdaTask([this](Task *ctx) {
      ctx->setTaskType(TaskType::kRelationalOperator);
      ctx->setProfiling(true);
      ctx->setCascade(true);
      ctx->setTaskMajorId(query_id_);

      this->execute(ctx);
    });
  }

  std::size_t op_index_;

  friend class ExecutionPlan;

  DISALLOW_COPY_AND_ASSIGN(RelationalOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_RELATIONAL_OPERATOR_HPP_
