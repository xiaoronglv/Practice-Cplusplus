#ifndef PROJECT_OPTIMIZER_QUERY_HANDLE_HPP_
#define PROJECT_OPTIMIZER_QUERY_HANDLE_HPP_

#include <cstddef>
#include <string>
#include <memory>

#include "optimizer/ExecutionPlan.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

class QueryHandle {
 public:
  explicit QueryHandle(const std::size_t query_id)
      : query_id_(query_id),
        execution_plan_(std::make_unique<ExecutionPlan>(query_id)) {}

  std::size_t getQueryId() const {
    return query_id_;
  }

  ExecutionPlan *getExecutionPlan() {
    return execution_plan_.get();
  }

  ExecutionPlan *releaseExecutionPlan() {
    return execution_plan_.release();
  }

  const std::string& getResultString() {
    return result_string_;
  }

  std::string* getResultStringMutable() {
    return &result_string_;
  }

 private:
  const std::size_t query_id_;
  std::unique_ptr<ExecutionPlan> execution_plan_;
  std::string result_string_;

  DISALLOW_COPY_AND_ASSIGN(QueryHandle);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_QUERY_HANDLE_HPP_
