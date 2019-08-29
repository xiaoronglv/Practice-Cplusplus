#include "operators/IndexJoinAggregateOperator.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/ExpressionSynthesizer.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/utility/ForeignKeySlotAccessor.hpp"
#include "operators/utility/KeyCountVector.hpp"
#include "scheduler/Task.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/ForeignKeyIndex.hpp"
#include "storage/IndexManager.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/Range.hpp"
#include "utility/ScopedArray.hpp"

#include "glog/logging.h"

namespace project {

FKPKIndexJoinAggregateOperator::FKPKIndexJoinAggregateOperator(
    const std::size_t query_id,
    const Relation &probe_relation,
    const Relation &build_relation,
    Relation *output_relation,
    const attribute_id probe_attribute,
    const attribute_id build_attribute,
    std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
    std::vector<JoinSide> &&aggregate_expression_sides,
    std::unique_ptr<Predicate> &&probe_filter_predicate,
    const bool use_foreign_key_index)
    : RelationalOperator(query_id),
      probe_relation_(probe_relation),
      build_relation_(build_relation),
      output_relation_(output_relation),
      probe_attribute_(probe_attribute),
      build_attribute_(build_attribute),
      aggregate_expressions_(std::move(aggregate_expressions)),
      aggregate_expression_sides_(std::move(aggregate_expression_sides)),
      probe_filter_predicate_(std::move(probe_filter_predicate)),
      use_foreign_key_index_(use_foreign_key_index),
      is_null_(true) {}

void FKPKIndexJoinAggregateOperator::execute(Task *ctx) {
  if (probe_relation_.getNumTuples() == 0 ||
      build_relation_.getNumTuples() == 0) {
    return;
  }

  sums_ = AtomicVector<std::uint64_t>(aggregate_expressions_.size());

  DCHECK_EQ(aggregate_expressions_.size(), aggregate_expression_sides_.size());
  for (std::size_t i = 0; i < aggregate_expressions_.size(); ++i) {
    const JoinSide side = aggregate_expression_sides_[i];
    if (side == JoinSide::kProbe) {
      probe_aggregate_expressions_.emplace_back(aggregate_expressions_[i].get());
      probe_sums_.emplace_back(&(sums_[i]));
    } else {
      DCHECK(side == JoinSide::kBuild);
      build_aggregate_expressions_.emplace_back(aggregate_expressions_[i].get());
      build_sums_.emplace_back(&(sums_[i]));
    }
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        if (use_foreign_key_index_) {
          DCHECK_EQ(1u, probe_relation_.getNumBlocks());
          this->executeProbeSideFKIndex(internal);
        } else {
          this->executeProbeSideFK(internal);
        }
      }),
      CreateLambdaTask([&] {
        this->finalize();
      })));
}

template <typename CountAccessor>
void FKPKIndexJoinAggregateOperator::executeBuildSideFKIndex(
    const CountAccessor &accessor) {
  DCHECK_EQ(1u, build_relation_.getNumBlocks());
  const StorageBlock &block = build_relation_.getBlock(0);

  DCHECK(!build_aggregate_expressions_.empty());
  const IndexManager &index_manager = build_relation_.getIndexManager();
  DCHECK(index_manager.hasPrimaryKeyIndex(build_attribute_));

  using ProbeCppType = typename CountAccessor::ValueType;

  std::vector<std::uint64_t> locals(build_aggregate_expressions_.size());

  InvokeOnTypeIDForCppType(
      build_relation_.getAttributeType(build_attribute_).getTypeID(),
      [&](auto ic) -> void {
    using BuildCppType = typename decltype(ic)::head;
    using PrimaryKeyIndexType = PrimaryKeyIndexImpl<BuildCppType>;

    const PrimaryKeyIndexType &pk_index =
        static_cast<const PrimaryKeyIndexType&>(
            index_manager.getPrimaryKeyIndex(build_attribute_));

    OrderedTupleIdSequence tuples;
    tuples.reserve(accessor.length());

    std::vector<std::uint32_t> counts;
    counts.reserve(accessor.length());

    for (ProbeCppType i = accessor.begin(); i != accessor.end(); ++i) {
      const std::uint32_t count = accessor.at(i);
      if (count != 0) {
        const tuple_id tuple = pk_index.slotAt(i);
        if (tuple != kInvalidTupleID) {
          tuples.emplace_back(tuple);
          counts.emplace_back(count);
        }
      }
    }

    for (std::size_t i = 0; i < build_aggregate_expressions_.size(); ++i) {
      const Scalar *scalar = build_aggregate_expressions_[i];
      auto sum = scalar->accumulateMultiply(block, tuples, counts);

      if (sum == nullptr) {
        DCHECK_EQ(0u, i);
        break;
      }

      this->is_null_.store(false, std::memory_order_relaxed);
      locals[i] = *sum;
    }
  });

  for (std::size_t i = 0; i < locals.size(); ++i) {
    build_sums_[i]->fetch_add(locals[i]);
  }
}

template <typename CountVector>
void FKPKIndexJoinAggregateOperator::executeBlockProbeSideFKBlock(
    Task *ctx, const std::shared_ptr<const StorageBlock> &block, CountVector *cv) {
  std::shared_ptr<TupleIdSequence> filter;
  if (probe_filter_predicate_ != nullptr) {
    filter.reset(probe_filter_predicate_->getAllMatches(*block, nullptr));
  }

  ctx->spawnLambdaTask([this, block, filter] {
    for (std::size_t i = 0; i < probe_aggregate_expressions_.size(); ++i) {
      const Scalar *scalar = this->probe_aggregate_expressions_[i];
      auto sum = scalar->accumulate(*block, filter.get());

      if (sum == nullptr) {
        DCHECK_EQ(0u, i);
        break;
      }

      this->probe_sums_[i]->fetch_add(*sum, std::memory_order_relaxed);
    }
  });

  InvokeOnColumnAccessorMakeShared(
        block->createColumnAccessor(probe_attribute_),
        [&](auto accessor) -> void {
    if (filter == nullptr) {
      for (std::size_t i = 0; i < accessor->getNumTuples(); ++i) {
        cv->increaseCount(accessor->at(i));
      }
    } else {
      for (const tuple_id tuple : *filter) {
        cv->increaseCount(accessor->at(tuple));
      }
    }
  });
}

template <typename ElementType>
void FKPKIndexJoinAggregateOperator::executeProbeSideFKHelper(
    Task *ctx, const Range range) {
  using CountVector = KeyCountVectorImpl<ElementType, true>;

  // TODO(robin-team): Need better parallelization ..
  std::shared_ptr<CountVector> cv =
      std::make_shared<CountVector>(range.begin(), range.size());

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, cv](Task *internal) {
        this->probe_relation_.forEachBlockPiece(
            FKPKIndexJoinAggregateOperator::kBatchSize,
            [&](auto block) {
          internal->spawnLambdaTask([this, block, cv](Task *internal) {
            this->executeBlockProbeSideFKBlock(
                internal,
                std::static_pointer_cast<const StorageBlock>(block), cv.get());
          });
        });
      }),
      CreateLambdaTask([this, cv](Task *internal) {
        const RangeSplitter splitter =
            RangeSplitter::CreateWithPartitionLength(
                cv->begin(), cv->end(), FKPKIndexJoinAggregateOperator::kBatchSize);
        for (const Range range : splitter) {
          internal->spawnLambdaTask([this, range, cv] {
            std::unique_ptr<CountVector> subset(
                static_cast<CountVector*>(cv->createSubset(range)));
            this->executeBuildSideFKIndex(*subset);
          });
        }
      })));
}

void FKPKIndexJoinAggregateOperator::executeProbeSideFK(Task *ctx) {
  const IndexManager &build_index_manager = build_relation_.getIndexManager();
  DCHECK(build_index_manager.hasPrimaryKeyIndex(build_attribute_));

  const Range range = build_index_manager.getPrimaryKeyIndex(build_attribute_)
                                         .getRange();

  std::size_t max_count = std::numeric_limits<std::uint32_t>::max();

  const IndexManager &probe_index_manager = probe_relation_.getIndexManager();
  if (probe_index_manager.hasForeignKeyIndex(probe_attribute_)) {
    max_count = probe_index_manager.getForeignKeyIndex(probe_attribute_)
                                   .getMaxCount();
  }

  if (max_count <= std::numeric_limits<std::uint8_t>::max()) {
    executeProbeSideFKHelper<std::uint8_t>(ctx, range);
  } else {
    executeProbeSideFKHelper<std::uint32_t>(ctx, range);
  }
}

void FKPKIndexJoinAggregateOperator::executeProbeSideFKIndex(Task *ctx) {
  const IndexManager &index_manager = probe_relation_.getIndexManager();
  DCHECK(index_manager.hasForeignKeyIndex(probe_attribute_));

  InvokeOnTypeIDForCppType(
      probe_relation_.getAttributeType(probe_attribute_).getTypeID(),
      [&](auto ic) -> void {
    using CppType = typename decltype(ic)::head;
    using ForeignKeyIndexType = ForeignKeyIndexImpl<CppType>;

    const ForeignKeyIndexType &fk_index =
        static_cast<const ForeignKeyIndexType&>(
            index_manager.getForeignKeyIndex(probe_attribute_));

    Range range = fk_index.getRange();
    bool exactness = true;

    if (probe_filter_predicate_ != nullptr) {
      range = probe_filter_predicate_->reduceRange(range, &exactness);
    }

    if (range.size() == 0) {
      return;
    }

    DCHECK(exactness);
    DCHECK_LE(fk_index.base(), range.begin());
    DCHECK_GE(fk_index.base() + fk_index.length(), range.end());

    if (!probe_aggregate_expressions_.empty()) {
      ctx->spawnLambdaTask([&, range] {
        DCHECK_EQ(1u, probe_aggregate_expressions_.size());
        const Scalar &scalar = *probe_aggregate_expressions_.front();
        DCHECK(scalar.getScalarType() == ScalarType::kAttribute);
        const ScalarAttribute &scalar_attribute =
            static_cast<const ScalarAttribute&>(scalar);
        DCHECK(scalar_attribute.attribute()->id() == probe_attribute_);
        (void) scalar_attribute;

        std::uint64_t sum = 0;
        for (CppType value = range.begin(); value < range.end(); ++value) {
          const std::uint32_t count = fk_index.slotAtUnchecked(value).second;
          if (count != 0) {
            sum += value * count;
          }
        }

        DCHECK_EQ(1u, probe_sums_.size());
        probe_sums_.front()->store(sum, std::memory_order_relaxed);
      });
    }

    using ForeignKeySlotAccessorType = ForeignKeySlotAccessor<CppType>;
    std::shared_ptr<ForeignKeySlotAccessorType> accessor =
        std::make_shared<ForeignKeySlotAccessorType>(
            range.begin(), range.size(),
            &fk_index.slotAtUnchecked(range.begin()));

    this->executeBuildSideFKIndex(*accessor);
  });
}

void FKPKIndexJoinAggregateOperator::finalize() {
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


BuildSideFKIndexJoinAggregateOperator::BuildSideFKIndexJoinAggregateOperator(
    const std::size_t query_id,
    const Relation &probe_relation,
    const Relation &build_relation,
    Relation *output_relation,
    const attribute_id probe_attribute,
    const attribute_id build_attribute,
    std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
    std::unique_ptr<Predicate> &&probe_filter_predicate,
    std::unique_ptr<Predicate> &&build_filter_predicate)
    : RelationalOperator(query_id),
      probe_relation_(probe_relation),
      build_relation_(build_relation),
      output_relation_(output_relation),
      probe_attribute_(probe_attribute),
      build_attribute_(build_attribute),
      aggregate_expressions_(std::move(aggregate_expressions)),
      probe_filter_predicate_(std::move(probe_filter_predicate)),
      build_filter_predicate_(std::move(build_filter_predicate)),
      is_null_(true) {}

void BuildSideFKIndexJoinAggregateOperator::execute(Task *ctx) {
  if (probe_relation_.getNumTuples() == 0 ||
      build_relation_.getNumTuples() == 0) {
    return;
  }

  sums_ = AtomicVector<std::uint64_t>(aggregate_expressions_.size());

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        probe_relation_.forEachBlockPiece(
            BuildSideFKIndexJoinAggregateOperator::kBatchSize,
            [&](auto block) -> void {
          internal->spawnLambdaTask([this, block] {
            this->executeBlock(*block);
          });
        });
      }),
      CreateLambdaTask([&] {
        this->finalize();
      })));
}

void BuildSideFKIndexJoinAggregateOperator::executeBlock(
    const StorageBlock &block) {
  const IndexManager &build_index_manager = build_relation_.getIndexManager();
  DCHECK(build_index_manager.hasForeignKeyIndex(build_attribute_));

  OrderedTupleIdSequence tuples;
  std::vector<std::uint32_t> counts;

  InvokeOnTypeIDForCppType(
      build_relation_.getAttributeType(build_attribute_).getTypeID(),
      [&](auto ic) -> void {
    using CppType = typename decltype(ic)::head;
    using ForeignKeyIndexType = ForeignKeyIndexImpl<CppType>;

    const ForeignKeyIndexType &fk_index =
        static_cast<const ForeignKeyIndexType&>(
            build_index_manager.getForeignKeyIndex(build_attribute_));

    Range range = fk_index.getRange();
    bool exactness = true;

    if (build_filter_predicate_ != nullptr) {
      range = build_filter_predicate_->reduceRange(range, &exactness);
    }

    if (range.size() == 0) {
      return;
    }

    DCHECK(exactness);
    DCHECK_LE(fk_index.base(), range.begin());
    DCHECK_GE(fk_index.base() + fk_index.length(), range.end());

    using ForeignKeySlotAccessorType = ForeignKeySlotAccessor<CppType>;

    std::unique_ptr<ForeignKeySlotAccessorType> fk_accessor =
        std::make_unique<ForeignKeySlotAccessorType>(
            range.begin(), range.size(),
            &fk_index.slotAtUnchecked(range.begin()));

    InvokeOnColumnAccessorMakeShared(
        block.createColumnAccessor(probe_attribute_),
        [&](auto accessor) -> void {
      if (probe_filter_predicate_ != nullptr) {
        std::unique_ptr<TupleIdSequence> filter(
            probe_filter_predicate_->getAllMatches(block, nullptr /* filter */));

        const std::size_t num_tuples = filter->getNumTuples();
        tuples.reserve(num_tuples);
        counts.reserve(num_tuples);

        for (const tuple_id tuple : *filter) {
          const std::uint32_t count = fk_accessor->atChecked(accessor->at(tuple));
          if (count > 0) {
            tuples.emplace_back(tuple);
            counts.emplace_back(count);
          }
        }
      } else {
        const std::size_t num_tuples = accessor->getNumTuples();
        tuples.reserve(num_tuples);
        counts.reserve(num_tuples);

        for (std::size_t i = 0; i < num_tuples; ++i) {
          const std::uint32_t count = fk_accessor->atChecked(accessor->at(i));
          if (count > 0) {
            tuples.emplace_back(i);
            counts.emplace_back(count);
          }
        }
      }
    });
  });

  if (tuples.empty()) {
    return;
  }

  for (std::size_t i = 0; i < aggregate_expressions_.size(); ++i) {
    const Scalar *scalar = aggregate_expressions_[i].get();
    auto sum = scalar->accumulateMultiply(block, tuples, counts);
    DCHECK(sum != nullptr);

    sums_[i].fetch_add(*sum);
  }
  this->is_null_.store(false, std::memory_order_relaxed);
}

void BuildSideFKIndexJoinAggregateOperator::finalize() {
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
