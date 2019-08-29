#include "operators/MultiwayJoinAggregateOperator.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "operators/aggregators/BuildKeyCountVector.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/utility/KeyCountVector.hpp"
#include "operators/utility/KeyCountVectorAccumulator.hpp"
#include "operators/utility/MultiwayJoinContext.hpp"
#include "scheduler/Task.hpp"
#include "storage/Attribute.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/Database.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/HashFilter.hpp"
#include "utility/Range.hpp"
#include "utility/meta/MultipleDispatcher.hpp"

#include "glog/logging.h"

namespace project {

MultiwaySymmetricJoinAggregateOperator::MultiwaySymmetricJoinAggregateOperator(
    const std::size_t query_id,
    const std::vector<const Relation*> &input_relations,
    Relation *output_relation,
    const Database &database,
    const std::vector<attribute_id> &join_attributes,
    const std::vector<std::uint32_t> &max_counts,
    const Range &join_attribute_range,
    std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
    std::vector<std::size_t> &&aggregate_expression_indexes,
    std::vector<std::unique_ptr<Predicate>> &&filter_predicates)
    : RelationalOperator(query_id),
      output_relation_(output_relation),
      join_attribute_range_(join_attribute_range),
      aggregate_expressions_(std::move(aggregate_expressions)),
      has_empty_input_(false),
      is_null_(true) {
  const std::size_t num_components = input_relations.size();
  DCHECK_EQ(num_components, join_attributes.size());
  DCHECK_EQ(num_components, max_counts.size());
  DCHECK_EQ(num_components, filter_predicates.size());

  for (std::size_t i = 0; i < num_components; ++i) {
    contexts_.emplace_back(
        std::make_unique<JoinContext>(*input_relations[i],
                                      join_attributes[i],
                                      std::move(filter_predicates[i]),
                                      max_counts[i]));
  }

  sums_ = AtomicVector<std::uint64_t>(aggregate_expressions_.size());

  DCHECK_EQ(aggregate_expressions_.size(), aggregate_expression_indexes.size());
  for (std::size_t i = 0; i < aggregate_expressions_.size(); ++i) {
    const std::size_t index = aggregate_expression_indexes[i];
    contexts_[index]->aggregate_expressions.emplace_back(
        aggregate_expressions_[i].get());
    contexts_[index]->sums.emplace_back(&(sums_[i]));
  }

  eliminateRedundantKeyCountVectors(database);
  eliminateRedundantBlockScan();
}

void MultiwaySymmetricJoinAggregateOperator::eliminateRedundantKeyCountVectors(
    const Database &database) {
  const std::size_t num_components = contexts_.size();
  for (std::size_t i = 0; i < num_components; ++i) {
    JoinContext *context = contexts_[i].get();
    if (context->filter_predicate != nullptr) {
      continue;
    }
    const attribute_id id = context->join_attribute;
    const Attribute *attribute = &context->relation.getAttribute(id);
    if (!attribute->getParentRelation().isPrimaryKey(id)) {
      continue;
    }

    bool is_redundant = true;
    for (std::size_t j = 0; j < num_components; ++j) {
      if (i == j) {
        continue;
      }
      const Attribute *other =
          &contexts_[j]->relation.getAttribute(contexts_[j]->join_attribute);
      if (!database.isPrimaryKeyForeignKey(attribute, other)) {
        is_redundant = false;
        break;
      }
    }
    if (is_redundant) {
      context->build_key_count_vector = false;
    }
  }
}

void MultiwaySymmetricJoinAggregateOperator::eliminateRedundantBlockScan() {
  for (auto &context : contexts_) {
    // Try to create the free-of-cost count vector.
    if (tryCreateCountVectorFromIndex(context.get())) {
      context->build_key_count_vector = false;
    }
    if (context->build_key_count_vector) {
      // We have to scan blocks to create the key count vector.
      continue;
    }

    const std::size_t num_output = context->aggregate_expressions.size();

    if (num_output == 0) {
      context->scan_blocks = false;
      continue;
    }

    if (num_output > 1 ||
        join_attribute_range_.size() > kKeyCountVectorScanThreshold) {
      continue;
    }

    // Accumulate on the key count vector to generate output.
    const Scalar &scalar = *context->aggregate_expressions.front();
    if (scalar.getScalarType() != ScalarType::kAttribute) {
      continue;
    }

    const attribute_id attribute =
        static_cast<const ScalarAttribute&>(scalar).attribute()->id();
    if (attribute == context->join_attribute) {
      context->scan_blocks = false;
    }
  }
}

void MultiwaySymmetricJoinAggregateOperator::execute(Task *ctx) {
  if (join_attribute_range_.size() == 0) {
    return;
  }

  for (const auto &context : contexts_) {
    if (context->relation.getNumTuples() == 0) {
      return;
    }
  }

  DCHECK(join_attribute_range_.valid());

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        for (auto &context : contexts_) {
          this->createJoinContext(internal, context.get());
        }
      }),
      CreateLambdaTask([&](Task *internal) {
        this->createLookaheadFilter(internal);
      }),
      CreateLambdaTask([&](Task *internal) {
        if (has_empty_input_) {
          return;
        }
        for (std::size_t i = 0; i < contexts_.size(); ++i) {
          internal->spawnLambdaTask([this, i](Task *internal) {
            this->createCountVector(internal, contexts_[i].get());
          });
        }
      }),
      CreateLambdaTask([&](Task *internal) {
        if (has_empty_input_) {
          return;
        }
        for (std::size_t i = 0; i < contexts_.size(); ++i) {
          std::vector<const KeyCountVector*> kcvs;
          for (std::size_t j = 0; j < contexts_.size(); ++j) {
            if (i != j) {
              const KeyCountVector *kcv = contexts_[j]->count_vector.get();
              if (kcv != nullptr) {
                kcvs.emplace_back(kcv);
              }
            }
          }
          KeyCountVectorAccumulator::Invoke(internal,
                                            contexts_[i].get(),
                                            join_attribute_range_,
                                            std::move(kcvs),
                                            lookahead_filter_.get(),
                                            &is_null_);
        }
      }),
      CreateLambdaTask([&] {
        this->finalize();
      })));
}

void MultiwaySymmetricJoinAggregateOperator::createJoinContext(
    Task *ctx, JoinContext *context) {
  if (!context->scan_blocks) {
    context->total_num_tuples.store(std::numeric_limits<std::size_t>::max(),
                                    std::memory_order_relaxed);
    return;
  }

  ctx->spawnLambdaTask([this, context](Task *internal) {
    context->relation.forEachBlockPiece(
        MultiwaySymmetricJoinAggregateOperator::kBatchSize,
        [&](auto block) {
      context->blocks.emplace_back(block);
    });

    if (context->filter_predicate != nullptr) {
      const std::size_t num_blocks = context->blocks.size();
      context->filters.resize(num_blocks);
      for (std::size_t i = 0; i < num_blocks; ++i) {
        internal->spawnLambdaTask([this, context, i] {
          this->createFilter(i, context);
        });
      }
    } else {
      context->total_num_tuples.fetch_add(context->relation.getNumTuples(),
                                          std::memory_order_relaxed);
    }
  });
}

void MultiwaySymmetricJoinAggregateOperator::createFilter(
    const std::size_t index, JoinContext *context) {
  const StorageBlock &block = *context->blocks[index];
  auto &filter = context->filters[index];

  DCHECK(context->filter_predicate != nullptr);
  filter.reset(context->filter_predicate->getAllMatches(block, nullptr));

  context->total_num_tuples.fetch_add(
      filter->getNumTuples(), std::memory_order_relaxed);
}


template <typename Accessor, typename TupleIdContainer>
void MultiwaySymmetricJoinAggregateOperator::createLookaheadFilterInternal(
    const Accessor &accessor,
    const TupleIdContainer &tuple_ids,
    ConcurrentHashFilter *hash_filter) {
  for (const tuple_id tid : tuple_ids) {
    hash_filter->insert(accessor.at(tid));
  }
}

void MultiwaySymmetricJoinAggregateOperator::createLookaheadFilter(Task *ctx) {
  std::size_t cardinality = 0;
  for (const auto &context : contexts_) {
    const std::size_t local_card = context->total_num_tuples;
    if (local_card == 0) {
      has_empty_input_.store(true, std::memory_order_relaxed);
      return;
    }
    if (local_card > kLookaheadFilterCardinalityThreshold) {
      continue;
    }
    cardinality = std::max(cardinality, local_card);
  }

  if (cardinality == 0) {
    return;
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, cardinality](Task *internal) {
        for (auto &context : this->contexts_) {
          if (context->total_num_tuples <= cardinality) {
            this->createLookaheadFilterContext(internal, context.get(), cardinality);
          }
        }
      }),
      CreateLambdaTask([&](Task *internal) {
        this->mergeLookaheadFilters(internal);
      })));
}

void MultiwaySymmetricJoinAggregateOperator::createLookaheadFilterContext(
    Task *ctx, JoinContext *context, const std::size_t cardinality) {
  context->lookahead_filter =
      std::make_unique<ConcurrentHashFilter>(cardinality * kLookaheadFilterBoostFactor);

  DCHECK(!context->blocks.empty());
  for (std::size_t j = 0; j < context->blocks.size(); ++j) {
    ctx->spawnLambdaTask([this, context, j] {
      const StorageBlock &block = *context->blocks[j];
      InvokeOnColumnAccessorMakeShared(
          block.createColumnAccessor(context->join_attribute),
          [&](auto accessor) {
        if (context->filter_predicate != nullptr) {
          this->createLookaheadFilterInternal(
              *accessor, *context->filters[j], context->lookahead_filter.get());
        } else {
          const auto tuples = Range(0, accessor->getNumTuples()).getGenerator();
          this->createLookaheadFilterInternal(
              *accessor, tuples, context->lookahead_filter.get());
        }
      });
    });
  }
}

void MultiwaySymmetricJoinAggregateOperator::mergeLookaheadFilters(Task *ctx) {
  // TODO(robin-team): May parallelize this if it becomes critical.
  for (const auto &context : contexts_) {
    if (context->lookahead_filter == nullptr) {
      continue;
    }
    if (lookahead_filter_ == nullptr) {
      lookahead_filter_.reset(context->lookahead_filter->clone<HashFilter>());
    } else {
      lookahead_filter_->intersectWith(*context->lookahead_filter);
    }
  }
}

void MultiwaySymmetricJoinAggregateOperator::createCountVector(
    Task *ctx, JoinContext *context) {
  if (!context->build_key_count_vector) {
    return;
  }

  std::shared_ptr<KeyCountVectorBuilder> builder(
      KeyCountVectorBuilder::Create(
          context->relation.getAttributeType(context->join_attribute),
          context->total_num_tuples,
          join_attribute_range_,
          context->max_count,
          lookahead_filter_.get(),
          &context->count_vector));

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([context, builder](Task *internal) {
        for (std::size_t i = 0; i < context->blocks.size(); ++i) {
          std::shared_ptr<ColumnAccessor> accessor(
              context->blocks[i]->createColumnAccessor(context->join_attribute));
          const TupleIdSequence *filter = nullptr;
          if (context->filter_predicate != nullptr) {
            filter = context->filters[i].get();
          }
          builder->accumulate(internal, accessor, filter);
        }
      }),
      CreateLambdaTask([builder](Task *internal) {
        builder->finalize(internal);
      }),
      CreateLambdaTask([builder] {})));
}

bool MultiwaySymmetricJoinAggregateOperator::tryCreateCountVectorFromIndex(
    JoinContext *context) {
  const IndexManager &index_manager = context->relation.getIndexManager();
  const attribute_id attribute = context->join_attribute;

  const bool unique = context->relation.isPrimaryKey(attribute);
  const bool has_kcv = index_manager.hasKeyCountVector(attribute);

  if (!unique && !has_kcv) {
    return false;
  }

  const Predicate *predicate = context->filter_predicate.get();
  if (predicate != nullptr) {
    if (!predicate->impliesExactRange(attribute)) {
      return false;
    }
    bool exactness = true;
    const Range range = predicate->reduceRange(join_attribute_range_, &exactness);
    CHECK(exactness);
    CHECK(range == join_attribute_range_);
  }

  if (has_kcv) {
    context->count_vector.reset(
        index_manager.getKeyCountVector(attribute)
                     .createSubset(join_attribute_range_));
  } else {
    DCHECK(unique);
    context->count_vector =
        std::make_unique<BitCountVector>(
            join_attribute_range_, index_manager.getExistenceMap(attribute));
  }
  return true;
}

void MultiwaySymmetricJoinAggregateOperator::finalize() {
  if (is_null_) {
    return;
  }

  std::unique_ptr<ColumnStoreBlock> block = std::make_unique<ColumnStoreBlock>();
  for (std::size_t i = 0; i < sums_.size(); ++i) {
    auto column = std::make_unique<ColumnVectorImpl<std::uint64_t>>(1u);
    column->appendValue(sums_[i]);
    block->addColumn(column.release());
  }
  output_relation_->addBlock(block.release());
}

}  // namespace project
