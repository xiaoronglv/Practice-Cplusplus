#ifndef PROJECT_WORKLOAD_TABLE_ANALYZER_HPP_
#define PROJECT_WORKLOAD_TABLE_ANALYZER_HPP_

#include "scheduler/Task.hpp"
#include "storage/Database.hpp"
#include "utility/Macros.hpp"

namespace project {

class TableAnalyzer {
 public:
  TableAnalyzer() {}

  void analyzeDatabase(Task *ctx, Database *database) const;

 private:
  void analyzeRelation(Task *ctx, Relation *relation) const;
  void analyzeColumn(Task *ctx, Relation *relation, const attribute_id id) const;

  void analyzePrimaryKeyForeignKey(Task *ctx, Database *database) const;
  void analyzePrimaryKeyForeignKeyHelper(Task *ctx,
                                         const Attribute *primary_key,
                                         Database *database) const;
  bool analyzePrimaryKeyForeignKeyPair(const Attribute *primary_key,
                                       const Attribute *target_attr) const;

  void buildForeignKeyIndexes(Task *ctx, Database *database) const;
  void buildPrimaryKeyIndexes(Task *ctx, Database *database) const;
  void buildKeyCountVectors(Task *ctx, Database *database) const;

  DISALLOW_COPY_AND_ASSIGN(TableAnalyzer);
};

}  // namespace project

#endif  // PROJECT_WORKLOAD_TABLE_ANALYZER_HPP_
