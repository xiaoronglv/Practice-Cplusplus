#ifndef PROJECT_OPERATORS_CREATE_TABLE_VIEW_OPERATOR_HPP_
#define PROJECT_OPERATORS_CREATE_TABLE_VIEW_OPERATOR_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Comparison.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {

class Task;

class CreateTableViewOperator : public RelationalOperator {
 public:
  CreateTableViewOperator(const std::size_t query_id,
                          const Relation &input_relation,
                          Relation *output_relation,
                          std::unique_ptr<Comparison> &&comparison,
                          const SortOrder sort_order)
      : RelationalOperator(query_id),
        input_relation_(input_relation),
        output_relation_(output_relation),
        comparison_(std::move(comparison)),
        sort_order_(sort_order) {}

  std::string getName() const override {
      return "CreateTableViewOperator";
  }

  const Relation& getInputRelation() const {
      return input_relation_;
  }

  const Relation& getOutputRelation() const {
      return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  const Relation &input_relation_;
  Relation *output_relation_;

  const std::unique_ptr<Comparison> comparison_;
  const SortOrder sort_order_;

  DISALLOW_COPY_AND_ASSIGN(CreateTableViewOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_CREATE_TABLE_VIEW_OPERATOR_HPP_
