#include "operators/ChainedJoinAggregateOperator.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/aggregators/BuildKeyCountVector.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/utility/KeyCountVector.hpp"
#include "scheduler/SchedulerInterface.hpp"
#include "scheduler/Task.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/IndexManager.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

ChainedJoinAggregateOperator::ChainedJoinAggregateOperator(
    const std::size_t query_id,
    const std::vector<const Relation*> &input_relations,
    Relation *output_relation,
    const std::vector<std::pair<attribute_id, attribute_id>> &join_attribute_pairs,
    std::vector<Range> &&join_attribute_ranges,
    std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
    std::vector<std::unique_ptr<Predicate>> &&filter_predicates)
    : RelationalOperator(query_id),
      output_relation_(output_relation),
      aggregate_expressions_(std::move(aggregate_expressions)),
      is_null_(true) {
  const std::size_t num_components = input_relations.size();
  const std::size_t num_joins = join_attribute_pairs.size();

  DCHECK_GE(num_components, 3u);
  DCHECK_EQ(num_components, num_joins + 1);
  DCHECK_EQ(num_joins, join_attribute_ranges.size());
  DCHECK_EQ(num_components, filter_predicates.size());

  for (std::size_t i = 0; i < num_components; ++i) {
    const attribute_id build_attribute =
        i == num_components - 1 ? kInvalidAttributeID : join_attribute_pairs[i].first;
    const attribute_id probe_attribute =
        i == 0 ? kInvalidAttributeID : join_attribute_pairs[i-1].second;

    const Range domain =
        i == num_components - 1 ? Range::Max() : join_attribute_ranges[i];

    contexts_.emplace_back(
        std::make_unique<JoinContext>(*input_relations[i],
                                      build_attribute,
                                      probe_attribute,
                                      domain,
                                      std::move(filter_predicates[i])));
  }

  sums_ = std::vector<std::atomic<std::uint64_t>>(aggregate_expressions_.size());
}

void ChainedJoinAggregateOperator::execute(Task *ctx) {
  for (const auto &context : contexts_) {
    if (context->relation.getNumTuples() == 0 || context->domain.size() == 0) {
      return;
    }
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        if (tryCreateFirstCountVectorFromIndex()) {
          return;
        }
        this->createJoinContext(internal, contexts_.front().get());
      }),
      CreateLambdaTask([&](Task *internal) {
        this->createFirstCountVector(internal);
      }),
      CreateLambdaTask([&](Task *internal) {
        this->executeChainJoins(internal);
      }),
      CreateLambdaTask([&](Task *internal) {
        this->accumulateOutput(internal);
      }),
      CreateLambdaTask([&](Task *internal) {
        this->finalize();
      })));
}

void ChainedJoinAggregateOperator::createFirstCountVector(Task *ctx) {
  JoinContext *context = contexts_.front().get();

  if (context->count_vector != nullptr) {
    return;
  }

  DCHECK_NE(context->build_attribute, kInvalidAttributeID);

  std::shared_ptr<KeyCountVectorBuilder> builder(
      KeyCountVectorBuilder::Create(
          context->relation.getAttributeType(context->build_attribute),
          context->total_num_tuples,
          context->domain,
          std::numeric_limits<std::uint32_t>::max(),
          nullptr,
          &context->count_vector));

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([context, builder](Task *internal) {
        for (std::size_t i = 0; i < context->blocks.size(); ++i) {
          std::shared_ptr<ColumnAccessor> accessor(
              context->blocks[i]->createColumnAccessor(context->build_attribute));
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

bool ChainedJoinAggregateOperator::tryCreateFirstCountVectorFromIndex() {
  JoinContext *context = contexts_.front().get();

  const IndexManager &index_manager = context->relation.getIndexManager();

  DCHECK(context->build_attribute != kInvalidAttributeID);
  DCHECK(context->probe_attribute == kInvalidAttributeID);
  const attribute_id attribute = context->build_attribute;

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
    const Range range = predicate->reduceRange(context->domain, &exactness);
    CHECK(exactness);
    CHECK(range == context->domain);
  }

  if (has_kcv) {
    context->count_vector.reset(
        index_manager.getKeyCountVector(attribute).createSubset(context->domain));
  } else {
    DCHECK(unique);
    context->count_vector =
        std::make_unique<BitCountVector>(
            context->domain, index_manager.getExistenceMap(attribute));
  }
  return true;
}

void ChainedJoinAggregateOperator::createFilter(
    const std::size_t index, JoinContext *context) {
  const StorageBlock &block = *context->blocks[index];
  auto &filter = context->filters[index];

  DCHECK(context->filter_predicate != nullptr);
  filter.reset(context->filter_predicate->getAllMatches(block, nullptr));

  context->total_num_tuples.fetch_add(
      filter->getNumTuples(), std::memory_order_relaxed);
}

void ChainedJoinAggregateOperator::executeChainJoins(Task *ctx) {
  SchedulerInterface *scheduler = ctx->getScheduler();

  const Continuation c_enter = scheduler->allocateContinuation();
  Continuation c_dependency = c_enter;

  for (std::size_t i = 1; i < contexts_.size()-1; ++i) {
    const Continuation c_dependent = scheduler->allocateContinuation();

    scheduler->addTask(
        CreateLambdaTask([this, i](Task *internal) {
          this->executeChainJoin(internal, i);
        }),
        c_dependency, c_dependent);

    c_dependency = c_dependent;
  }

  scheduler->addLink(c_dependency, ctx->getContinuation());
  scheduler->fire(c_enter);
}

template <typename BuildAccessor, typename ProbeAccessor,
          typename TupleIdContainer,
          typename InputCountVector, typename OutputCountVector>
void ChainedJoinAggregateOperator::accumulateCountVectorBlock(
    const Range &build_domain,
    const BuildAccessor &build_accessor,
    const ProbeAccessor &probe_accessor,
    const TupleIdContainer &tuple_ids,
    const InputCountVector &input_kcv,
    OutputCountVector *output_kcv) {
  for (const tuple_id tid : tuple_ids) {
    const std::uint32_t count = input_kcv.atChecked(probe_accessor.at(tid));
    const auto &build_value = build_accessor.at(tid);
    if (count != 0 && build_domain.contains(build_value)) {
      output_kcv->increaseCount(build_value, count);
    }
  }
}

template <typename InputCountVector, typename OutputCountVector>
void ChainedJoinAggregateOperator::accumulateCountVectorDispatch(
    Task *ctx, JoinContext *context,
    const InputCountVector &input_kcv,
    OutputCountVector *output_kcv) {
  DCHECK(context->build_attribute != kInvalidAttributeID);
  DCHECK(context->probe_attribute != kInvalidAttributeID);

  for (std::size_t i = 0; i < context->blocks.size(); ++i) {
    const StorageBlock &block = *context->blocks[i];

    const TupleIdSequence *filter = nullptr;
    if (context->filter_predicate != nullptr) {
      DCHECK_LT(i, context->filters.size());
      DCHECK(context->filters[i] != nullptr);
      filter = context->filters[i].get();
    }

    ctx->spawnLambdaTask([this, context, &block, filter, &input_kcv, output_kcv] {
      InvokeOnColumnAccessorMakeShared(
          block.createColumnAccessor(context->build_attribute),
          [&](auto build_accessor) {
        InvokeOnColumnAccessorMakeShared(
            block.createColumnAccessor(context->probe_attribute),
            [&](auto probe_accessor) {
          if (filter != nullptr) {
            this->accumulateCountVectorBlock(
                context->domain, *build_accessor, *probe_accessor, *filter,
                input_kcv, output_kcv);
          } else {
            this->accumulateCountVectorBlock(
                context->domain, *build_accessor, *probe_accessor,
                Range(0, block.getNumTuples()).getGenerator(),
                input_kcv, output_kcv);
          }
        });
      });
    });
  }
}

template <bool input_atomic>
void ChainedJoinAggregateOperator::accumulateCountVectorInChain(
    Task *ctx, const std::size_t index) {
  DCHECK_GE(index, 1u);
  JoinContext *context = contexts_[index].get();

  const KeyCountVector *lookup_kcv = contexts_[index-1]->count_vector.get();
  DCHECK(lookup_kcv != nullptr);

  using OutputKeyCountVector = KeyCountVectorImpl<std::uint32_t, true>;
  OutputKeyCountVector *output_kcv =
      new OutputKeyCountVector(context->domain, false);

  context->count_vector.reset(output_kcv);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([output_kcv](Task *internal) {
        output_kcv->initialize(internal);
      }),
      CreateLambdaTask([this, context, lookup_kcv, output_kcv](Task *internal) {
        InvokeOnKeyCountVector<input_atomic>(
            *lookup_kcv,
            [&](const auto &input_kcv) {
          this->accumulateCountVectorDispatch(
              internal, context, input_kcv, output_kcv);
        });
      })));
}

void ChainedJoinAggregateOperator::executeChainJoin(
    Task *ctx, const std::size_t index) {
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, index](Task *internal) {
        this->createJoinContext(internal, contexts_[index].get());
      }),
      CreateLambdaTask([this, index](Task *internal) {
        DCHECK_GE(index, 1u);
        if (contexts_[index-1]->count_vector->isAtomic()) {
          this->accumulateCountVectorInChain<true>(internal, index);
        } else {
          this->accumulateCountVectorInChain<false>(internal, index);
        }
      })));
}

void ChainedJoinAggregateOperator::createJoinContext(
    Task *ctx, JoinContext *context) {
  context->relation.forEachBlockPiece(
      ChainedJoinAggregateOperator::kBatchSize,
      [&](auto block) {
    context->blocks.emplace_back(block);
  });

  if (context->filter_predicate != nullptr) {
    const std::size_t num_blocks = context->blocks.size();
    context->filters.resize(num_blocks);
    for (std::size_t i = 0; i < num_blocks; ++i) {
      ctx->spawnLambdaTask([this, context, i] {
        this->createFilter(i, context);
      });
    }
  } else {
    context->total_num_tuples.fetch_add(context->relation.getNumTuples(),
                                        std::memory_order_relaxed);
  }
}

template <typename Accessor, typename TupleIdContainer, typename CountVector>
void ChainedJoinAggregateOperator::accumulateOutputBlock(
    const StorageBlock &block,
    const Accessor &accessor,
    const TupleIdContainer &tuple_ids,
    const CountVector &cv) {
  OrderedTupleIdSequence tuples;
  std::vector<std::uint64_t> counts;

  const std::size_t max_num_tuples = accessor.getNumTuples();
  tuples.reserve(max_num_tuples);
  counts.reserve(max_num_tuples);

  for (const tuple_id tid : tuple_ids) {
    const std::uint64_t count = cv.atChecked(accessor.at(tid));
    if (count != 0) {
      tuples.emplace_back(tid);
      counts.emplace_back(count);
    }
  }

  if (tuples.empty()) {
    return;
  }

  for (std::size_t i = 0; i < aggregate_expressions_.size(); ++i) {
    const Scalar &scalar = *aggregate_expressions_[i];
    auto sum = scalar.accumulateMultiply(block, tuples, counts);
    DCHECK(sum != nullptr);

    sums_[i].fetch_add(*sum, std::memory_order_relaxed);
  }
  is_null_.store(false, std::memory_order_relaxed);
}

void ChainedJoinAggregateOperator::accumulateOutputDispatch(Task *ctx) {
  JoinContext *context = contexts_.back().get();
  DCHECK(context->build_attribute == kInvalidAttributeID);
  DCHECK(context->probe_attribute != kInvalidAttributeID);

  const std::size_t last_index = contexts_.size() - 1;
  DCHECK_GE(last_index, 2u);

  const KeyCountVector *lookup_kcv = contexts_[last_index-1]->count_vector.get();
  DCHECK(lookup_kcv != nullptr);

  for (std::size_t i = 0; i < context->blocks.size(); ++i) {
    const StorageBlock &block = *context->blocks[i];

    const TupleIdSequence *filter = nullptr;
    if (context->filter_predicate != nullptr) {
      DCHECK_LT(i, context->filters.size());
      DCHECK(context->filters[i] != nullptr);
      filter = context->filters[i].get();
    }

    ctx->spawnLambdaTask([this, context, lookup_kcv, &block, filter] {
      InvokeOnColumnAccessorMakeShared(
          block.createColumnAccessor(context->probe_attribute),
          [&](auto accessor) {
        InvokeOnKeyCountVector<true>(
            *lookup_kcv,
            [&](const auto &cv) {
          if (filter != nullptr) {
            this->accumulateOutputBlock(block, *accessor, *filter, cv);
          } else {
            const auto tuple_ids = Range(0, block.getNumTuples()).getGenerator();
            this->accumulateOutputBlock(block, *accessor, tuple_ids, cv);
          }
        });
      });
    });
  }
}

void ChainedJoinAggregateOperator::accumulateOutput(Task *ctx) {
  JoinContext *context = contexts_.back().get();

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, context](Task *internal) {
        this->createJoinContext(internal, context);
      }),
      CreateLambdaTask([this](Task *internal) {
        this->accumulateOutputDispatch(internal);
      })));
}

void ChainedJoinAggregateOperator::finalize() {
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
