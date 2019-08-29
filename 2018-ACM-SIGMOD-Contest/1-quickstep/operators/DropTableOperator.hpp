#ifndef PROJECT_OPERATORS_DROP_TABLE_OPERATOR_HPP_
#define PROJECT_OPERATORS_DROP_TABLE_OPERATOR_HPP_

#include <cstddef>
#include <string>

#include "operators/RelationalOperator.hpp"
#include "storage/Database.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {

class Task;

class DropTableOperator : public RelationalOperator {
 public:
  DropTableOperator(const std::size_t query_id,
                    const Relation &relation,
                    Database *database)
      : RelationalOperator(query_id),
        relation_(relation),
        database_(database) {}

  std::string getName() const override {
    return "DropTableOperator";
  }

  const Relation& getRelation() const {
    return relation_;
  }

  void execute(Task *ctx) override {
    database_->dropRelation(relation_.getID());
  }

 private:
  const Relation &relation_;
  Database *database_;

  DISALLOW_COPY_AND_ASSIGN(DropTableOperator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_DROP_TABLE_OPERATOR_HPP_
