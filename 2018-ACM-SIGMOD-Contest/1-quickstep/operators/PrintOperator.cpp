#include "operators/PrintOperator.hpp"

#include <cstddef>
#include <string>
#include <vector>

#include "utility/StringUtil.hpp"

namespace project {

void PrintSingleTupleToStringOperator::execute(Task *ctx) {
  const std::size_t num_columns = input_relation_.getNumAttributes();
  const std::size_t num_tuples = input_relation_.getNumTuples();

  DCHECK_LE(num_tuples, 1u);
  if (num_tuples == 0) {
    *output_string_ = ConcatToString(
        std::vector<std::string>(num_columns, "NULL"), " ");
    return;
  }

  std::vector<std::string> values;
  for (std::size_t i = 0; i < num_columns; ++i) {
    input_relation_.forEachBlock(
        i, [&values](auto accessor) -> void {
      values.emplace_back(std::to_string(accessor->at(0)));
    });
  }
  *output_string_ = ConcatToString(values, " ");
}

}  // namespace project
