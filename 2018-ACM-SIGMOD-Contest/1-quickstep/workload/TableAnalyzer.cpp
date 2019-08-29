#include "workload/TableAnalyzer.hpp"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>

#include "operators/aggregators/AggregationIsSorted.hpp"
#include "operators/aggregators/AggregationMinMax.hpp"
#include "operators/aggregators/BuildKeyCountVector.hpp"
#include "operators/aggregators/BuildExistenceMap.hpp"
#include "operators/aggregators/BuildForeignKeyIndex.hpp"
#include "operators/aggregators/BuildPrimaryKeyIndex.hpp"
#include "operators/aggregators/BuildRangedPartitions.hpp"
#include "operators/aggregators/CompressColumn.hpp"
#include "scheduler/Task.hpp"
#include "storage/Attribute.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/ExistenceMap.hpp"
#include "storage/Relation.hpp"
#include "types/Type.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/SyncStream.hpp"

#include "gflags/gflags.h"

namespace project {

DEFINE_bool(pk_fk_pre_filter, false,
            "Whether to pre-filter column containment check pairs");
DEFINE_uint64(pk_fk_filter_margin, 100,
              "The pre-filtering min/max margin for column containment check");

DEFINE_uint64(build_fk_index_database_threshold, 10000000,
              "The database cardinality threshold for building all foreign key "
              "indexes");

DEFINE_uint64(build_pk_index_database_threshold, 200000000,
              "The table cardinality threshold for building all primary key "
              "indexes");
DEFINE_uint64(build_pk_index_table_threshold, 1,
              "The table cardinality threshold for building primary key "
              "indexes");

DEFINE_uint64(build_fk_count_vector_database_threshold, 2000000000,
              "The database cardinality threshold for building all foreign key "
              "count vectors");
DEFINE_uint64(build_fk_count_vector_table_threshold, 1,
              "The table cardinality threshold for building foreign key count "
              "vectors");

void TableAnalyzer::analyzeDatabase(Task *ctx, Database *database) const {
  ctx->setTaskType(TaskType::kPreprocessing);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, database](Task *internal) {
        for (std::size_t i = 0; i < database->getNumRelations(); ++i) {
          this->analyzeRelation(internal, database->getRelationMutable(i));
        }
      }),
      CreateLambdaTask([this, database](Task *internal) {
        this->analyzePrimaryKeyForeignKey(internal, database);
      }),
      CreateLambdaTask([this, database](Task *internal) {
        this->buildForeignKeyIndexes(internal, database);
        this->buildPrimaryKeyIndexes(internal, database);
        this->buildKeyCountVectors(internal, database);
      })));
}

void TableAnalyzer::analyzeRelation(Task *ctx, Relation *relation) const {
  if (relation->getStatistics().getNumTuples() == 0) {
    return;
  }

  for (std::size_t i = 0; i < relation->getNumAttributes(); ++i) {
    this->analyzeColumn(ctx, relation, i);
  }
}

void TableAnalyzer::analyzeColumn(
    Task *ctx, Relation *relation, const attribute_id id) const {
  ctx->setProfiling(true);
  ctx->setCascade(true);
  ctx->setTaskMajorId((relation->getID() << 16) | id);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, relation, id](Task *internal) {
        CompressColumn::InvokeWithCallback(
            internal, relation, id,
            [relation, id](auto &column, const Type &type,
                           const std::uint64_t min_value,
                           const std::uint64_t max_value) {
          AttributeStatistics &stat =
              relation->getStatisticsMutable()
                      ->getAttributeStatisticsMutable(id);
          // NOTE(robin-team): Must round min_value down to multiples of 64 to
          // guarantee alignment of existence map's internal data.
          stat.setMinValue(ExistenceMap::RoundDown64(min_value));
          stat.setMaxValue(max_value);

          // Use optimistic column compression w.r.t. memory bandwidth ...
          if (max_value < std::numeric_limits<std::uint32_t>::max()) {
            relation->replaceColumnn(id, type, column.release());
          }
        });
      }),
      CreateLambdaTask([this, relation, id](Task *internal) {
        BuildRangedPartitions::Invoke(internal, relation, id);
      }),
      CreateLambdaTask([this, relation, id](Task *internal) {
        AggregationIsSorted::InvokeWithCallback(
            internal, relation, id,
            [relation, id](const SortOrder sort_order) {
          relation->getStatisticsMutable()
                  ->getAttributeStatisticsMutable(id)
                   .setSortOrder(sort_order);
        });
        BuildExistenceMap::InvokeWithCallback(
            internal, relation, id,
            [relation, id](auto &existence_map) {
          relation->getStatisticsMutable()
                  ->getAttributeStatisticsMutable(id)
                   .setNumDistinctValues(existence_map->onesCount());
          relation->getIndexManagerMutable()
                  ->setExistenceMap(id, existence_map.release());
        });
      })
  ));  // NOLINT[whitespace/parens]
}

void TableAnalyzer::analyzePrimaryKeyForeignKey(
    Task *ctx, Database *database) const {
  for (std::size_t pk_rid = 0; pk_rid < database->getNumRelations(); ++pk_rid) {
    const Relation &pk_relation = database->getRelation(pk_rid);
    for (std::size_t pk_cid = 0; pk_cid < pk_relation.getNumAttributes(); ++pk_cid) {
      if (pk_relation.isPrimaryKey(pk_cid)) {
        analyzePrimaryKeyForeignKeyHelper(
            ctx, &pk_relation.getAttribute(pk_cid), database);
      }
    }
  }
}

void TableAnalyzer::analyzePrimaryKeyForeignKeyHelper(
    Task *ctx, const Attribute *primary_key, Database *database) const {
  for (std::size_t fk_rid = 0; fk_rid < database->getNumRelations(); ++fk_rid) {
    const Relation &fk_relation = database->getRelation(fk_rid);
    for (std::size_t fk_cid = 0; fk_cid < fk_relation.getNumAttributes(); ++fk_cid) {
      const Attribute *target = &fk_relation.getAttribute(fk_cid);
      if (primary_key != target) {
        ctx->spawnLambdaTask([this, database, primary_key, target] {
          if (this->analyzePrimaryKeyForeignKeyPair(primary_key, target)) {
            database->setPrimaryKeyForeignKey(primary_key, target);
          }
        });
      }
    }
  }
}

bool TableAnalyzer::analyzePrimaryKeyForeignKeyPair(
    const Attribute *primary_key, const Attribute *target_attr) const {
  const Relation &pk_relation = primary_key->getParentRelation();
  const Relation &fk_relation = target_attr->getParentRelation();

  if (!pk_relation.getIndexManager().hasExistenceMap(primary_key->id()) ||
      !fk_relation.getIndexManager().hasExistenceMap(target_attr->id())) {
    return false;
  }

  const AttributeStatistics &pk_stat =
      pk_relation.getStatistics().getAttributeStatistics(primary_key->id());
  const AttributeStatistics &fk_stat =
      fk_relation.getStatistics().getAttributeStatistics(target_attr->id());

  if (fk_stat.getMinValue() < pk_stat.getMinValue() ||
      fk_stat.getMaxValue() > pk_stat.getMaxValue() ||
      fk_stat.getNumDistinctValues() > pk_stat.getNumDistinctValues()) {
    return false;
  }

  // It seems that we don't need the pre-filtering ..
  if (FLAGS_pk_fk_pre_filter) {
    if (fk_stat.getMinValue() > pk_stat.getMinValue() + FLAGS_pk_fk_filter_margin ||
        fk_stat.getMaxValue() + FLAGS_pk_fk_filter_margin < pk_stat.getMaxValue()) {
      return false;
    }
  }

  const ExistenceMap &pk_em =
      pk_relation.getIndexManager().getExistenceMap(primary_key->id());
  const ExistenceMap &fk_em =
      fk_relation.getIndexManager().getExistenceMap(target_attr->id());

  return fk_em.isSubsetOf(pk_em);
}

void TableAnalyzer::buildForeignKeyIndexes(Task *ctx, Database *database) const {
  if (database->getTotalNumTuples() > FLAGS_build_fk_index_database_threshold) {
    return;
  }

  database->forEachForeignKey([&](const Attribute *attribute){
    ctx->spawnLambdaTask([this, attribute](Task *internal) {
      Relation *relation = attribute->getParentRelationMutable();
      const attribute_id id = attribute->id();
      BuildForeignKeyIndex::InvokeWithCallback(
          internal, relation, id,
          [relation, id](auto &fk_index) {
        relation->getIndexManagerMutable()
                ->setForeignKeyIndex(id, fk_index.release());
      });
    });
  });
}

void TableAnalyzer::buildPrimaryKeyIndexes(Task *ctx, Database *database) const {
  const bool build_all =
      database->getTotalNumTuples() <= FLAGS_build_pk_index_database_threshold;

  database->forEachPrimaryKey([&](const Attribute *attribute) {
    const std::size_t num_tuples =
        attribute->getParentRelation().getNumTuples();
    if (!build_all && num_tuples > FLAGS_build_pk_index_table_threshold) {
      return;
    }
    ctx->spawnLambdaTask([this, attribute](Task *internal) {
      Relation *relation = attribute->getParentRelationMutable();
      const attribute_id id = attribute->id();
      BuildPrimaryKeyIndex::InvokeWithCallback(
          internal, relation, id,
          [relation, id](auto &pk_index) {
        relation->getIndexManagerMutable()
                ->setPrimaryKeyIndex(id, pk_index.release());
      });
    });
  });
}

void TableAnalyzer::buildKeyCountVectors(Task *ctx, Database *database) const {
  const std::size_t total_num_tuples = database->getTotalNumTuples();

  if (total_num_tuples <= FLAGS_build_fk_index_database_threshold) {
    // KeyCountVector will be generated as side products of foreign key indexes.
    return;
  }

  const bool build_all =
      total_num_tuples <= FLAGS_build_fk_count_vector_database_threshold;

  database->forEachForeignKey([&](const Attribute *attribute) {
    const std::size_t num_tuples =
        attribute->getParentRelation().getNumTuples();
    if (!build_all && num_tuples > FLAGS_build_fk_count_vector_table_threshold) {
      return;
    }
    ctx->spawnLambdaTask([this, attribute](Task *internal) {
      Relation *relation = attribute->getParentRelationMutable();
      const attribute_id id = attribute->id();
      BuildKeyCountVector::InvokeWithCallback(
          internal, relation, id,
          [relation, id](auto &kcv) {
        relation->getIndexManagerMutable()
                ->setKeyCountVector(id, kcv.release());
      });
    });
  });
}

}  // namespace project
