#ifndef PROJECT_OPERATORS_SELECT_OPERATOR_HPP_
#define PROJECT_OPERATORS_SELECT_OPERATOR_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Comparison.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {

class Attribute;
class Task;

class BasicSelectOperator : public RelationalOperator {
 public:
  BasicSelectOperator(
      const std::size_t query_id,
      const Relation &input_relation,
      Relation *output_relation,
      std::vector<std::unique_ptr<Scalar>> &&project_expressions,
      std::unique_ptr<Predicate> &&filter_predicate);

  std::string getName() const override {
    return "BasicSelectOperator";
  }

  const Relation& getInputRelation() const {
    return input_relation_;
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  void executeBlock(const StorageBlock &block);

  const Relation &input_relation_;
  Relation *output_relation_;

  const std::vector<std::unique_ptr<Scalar>> project_expressions_;
  const std::unique_ptr<Predicate> filter_predicate_;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(BasicSelectOperator);
};


class IndexScanOperator : public RelationalOperator {
 public:
  IndexScanOperator(
      const std::size_t query_id,
      const Relation &input_relation,
      Relation *output_relation,
      std::vector<std::unique_ptr<Scalar>> &&project_expressions,
      std::unique_ptr<Comparison> &&comparison);

  std::string getName() const override {
    return "IndexScanOperator";
  }

  const Relation& getInputRelation() const {
    return input_relation_;
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  void executePrimaryKey(const Attribute *attribute, const std::uint64_t value);
  void executeForeignKey(const Attribute *attribute, const std::uint64_t value);

  const Relation &input_relation_;
  Relation *output_relation_;

  const std::vector<std::unique_ptr<Scalar>> project_expressions_;
  const std::unique_ptr<Comparison> comparison_;

  DISALLOW_COPY_AND_ASSIGN(IndexScanOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_SELECT_OPERATOR_HPP_
