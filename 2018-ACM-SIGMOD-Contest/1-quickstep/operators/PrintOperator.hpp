#ifndef PROJECT_OPERATORS_PRINT_OPERATOR_HPP_
#define PROJECT_OPERATORS_PRINT_OPERATOR_HPP_

#include <cstddef>
#include <string>

#include "operators/RelationalOperator.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {

class Task;

class PrintSingleTupleToStringOperator : public RelationalOperator {
 public:
  PrintSingleTupleToStringOperator(const std::size_t query_id,
                                   const Relation &input_relation,
                                   std::string *output_string)
      : RelationalOperator(query_id),
        input_relation_(input_relation),
        output_string_(output_string) {}

  std::string getName() const override {
    return "PrintSingleTupleToStringOperator";
  }

  const Relation& getInputRelation() const {
    return input_relation_;
  }

  void execute(Task *ctx) override;

 private:
  const Relation &input_relation_;
  std::string *output_string_;

  DISALLOW_COPY_AND_ASSIGN(PrintSingleTupleToStringOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_PRINT_OPERATOR_HPP_
