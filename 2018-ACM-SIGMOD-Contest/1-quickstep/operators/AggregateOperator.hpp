#ifndef PROJECT_OPERATORS_AGGREGATE_OPERATOR_HPP_
#define PROJECT_OPERATORS_AGGREGATE_OPERATOR_HPP_

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "storage/Relation.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/Macros.hpp"

namespace project {

class Task;
class Type;

class BasicAggregateOperator : public RelationalOperator {
 public:
  BasicAggregateOperator(const std::size_t query_id,
                         const Relation &input_relation,
                         std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
                         std::unique_ptr<Predicate> &&filter_predicate,
                         Relation *output_relation)
      : RelationalOperator(query_id),
        input_relation_(input_relation),
        aggregate_expressions_(std::move(aggregate_expressions)),
        filter_predicate_(std::move(filter_predicate)),
        output_relation_(output_relation),
        is_null_(true) {}

  std::string getName() const override {
    return "BasicAggregateOperator";
  }

  const Relation& getInputRelation() const {
    return input_relation_;
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  void executeBlock(const StorageBlock &block,
                    const SharedVector<std::atomic<std::uint64_t>> &sums);

  void executeGeneric(const StorageBlock &block,
                      std::vector<std::uint64_t> *locals);

  void finalize(const std::vector<std::atomic<std::uint64_t>> &sums) const;

  const Relation &input_relation_;
  const std::vector<std::unique_ptr<Scalar>> aggregate_expressions_;
  const std::unique_ptr<Predicate> filter_predicate_;
  Relation *output_relation_;

  std::atomic<bool> is_null_;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(BasicAggregateOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_AGGREGATE_OPERATOR_HPP_
