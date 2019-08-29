#ifndef PROJECT_OPERATORS_GENERAL_EQUI_JOIN_OPERATOR_HPP_
#define PROJECT_OPERATORS_GENERAL_EQUI_JOIN_OPERATOR_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "operators/OperatorTypedefs.hpp"
#include "operators/HashJoinOperator.hpp"
#include "operators/RelationalOperator.hpp"
#include "operators/SortMergeJoinOperator.hpp"
#include "operators/expressions/Predicate.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

namespace project {

class Relation;
class Task;

class GeneralEquiJoinOperator : public RelationalOperator {
 public:
  GeneralEquiJoinOperator(
      const std::size_t query_id,
      const Relation *probe_relation,
      const Relation *build_relation,
      Relation *output_relation,
      attribute_id probe_attribute,
      attribute_id build_attribute,
      std::vector<attribute_id> &&project_attributes,
      std::vector<JoinSide> &&project_attibute_sides,
      std::unique_ptr<Predicate> &&probe_filter_predicate,
      std::unique_ptr<Predicate> &&build_filter_predicate);

  std::string getName() const override {
    return "GeneralEquiJoinOperator";
  }

  void execute(Task *ctx) override;

 private:
  enum JoinOperatorType {
    kBasicHashJoin = 0,
    kSortMergeJoin
  };

  template <typename ...Args>
  void initialize(const JoinOperatorType type, Args &&...args);

  template <typename JoinOperator, typename ...Args>
  void initialize(std::unique_ptr<JoinOperator> *join, Args &&...args);

  void swapSide(const Relation **probe_relation,
                const Relation **build_relation,
                attribute_id *probe_attribute,
                attribute_id *build_attribute,
                std::vector<JoinSide> *project_attibute_sides,
                std::unique_ptr<Predicate> *probe_filter_predicate,
                std::unique_ptr<Predicate> *build_filter_predicate);

  std::unique_ptr<JoinOperatorType> join_type_;
  std::unique_ptr<BasicHashJoinOperator> basic_hash_join_;
  std::unique_ptr<SortMergeJoinOperator> sort_merge_join_;

  static constexpr std::uint64_t kBasicHashJoinCardinalityThreshold = 10000u;

  DISALLOW_COPY_AND_ASSIGN(GeneralEquiJoinOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_GENERAL_EQUI_JOIN_OPERATOR_HPP_

