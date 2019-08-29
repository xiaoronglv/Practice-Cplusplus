#ifndef PROJECT_OPERATORS_INDEX_JOIN_OPERATOR_HPP_
#define PROJECT_OPERATORS_INDEX_JOIN_OPERATOR_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "operators/OperatorTypedefs.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

namespace project {

class StorageBlock;
class Task;

class PrimaryKeyIndexJoinOperator : public RelationalOperator {
 public:
  PrimaryKeyIndexJoinOperator(
      const std::size_t query_id,
      const Relation &probe_relation,
      const Relation &build_relation,
      Relation *output_relation,
      const attribute_id probe_attribute,
      const attribute_id build_attribute,
      std::vector<std::unique_ptr<Scalar>> &&project_expressions,
      std::vector<JoinSide> &&project_expression_sides,
      std::unique_ptr<Predicate> &&probe_filter_predicate);

  std::string getName() const override {
    return "PrimaryKeyIndexJoinOperator";
  }

  const Relation& getProbeRelation() const {
    return probe_relation_;
  }

  const Relation& getBuildRelation() const {
    return build_relation_;
  }

  const Relation& getOutputRelation() const {
    return *output_relation_;
  }

  void execute(Task *ctx) override;

 private:
  void executeBlock(const StorageBlock &block) const;

  const Relation &probe_relation_;
  const Relation &build_relation_;
  Relation *output_relation_;

  const attribute_id probe_attribute_;
  const attribute_id build_attribute_;

  const std::vector<std::unique_ptr<Scalar>> project_expressions_;
  const std::vector<JoinSide> project_expression_sides_;

  const std::unique_ptr<Predicate> probe_filter_predicate_;

  static constexpr std::size_t kBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(PrimaryKeyIndexJoinOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_INDEX_JOIN_OPERATOR_HPP_
