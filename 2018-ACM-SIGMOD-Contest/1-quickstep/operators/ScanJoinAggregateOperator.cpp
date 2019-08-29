#include "operators/ScanJoinAggregateOperator.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Comparison.hpp"
#include "operators/expressions/ComparisonType.hpp"
#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/expressions/ScalarLiteral.hpp"
#include "scheduler/Task.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"

#include "glog/logging.h"

namespace project {

FKPKScanJoinAggregateOperator::FKPKScanJoinAggregateOperator(
    const std::size_t query_id,
    const Relation &probe_relation,
    const Relation &build_relation,
    Relation *output_relation,
    const attribute_id probe_attribute,
    const attribute_id build_attribute,
    std::vector<std::unique_ptr<Scalar>> &&aggregate_expressions,
    std::vector<JoinSide> &&aggregate_expression_sides,
    std::unique_ptr<Predicate> &&probe_filter_predicate,
    std::unique_ptr<Predicate> &&build_filter_predicate)
    : RelationalOperator(query_id),
      probe_relation_(probe_relation),
      build_relation_(build_relation),
      output_relation_(output_relation),
      probe_attribute_(probe_attribute),
      build_attribute_(build_attribute),
      aggregate_expressions_(std::move(aggregate_expressions)),
      aggregate_expression_sides_(std::move(aggregate_expression_sides)),
      probe_filter_predicate_(std::move(probe_filter_predicate)),
      build_filter_predicate_(std::move(build_filter_predicate)),
      is_null_(true) {
  DCHECK(build_filter_predicate_);
}

void FKPKScanJoinAggregateOperator::execute(Task *ctx) {
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

  build_sums_base_.resize(build_sums_.size());

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this](Task *internal) {
        this->executeBuildSidePKScan(internal);
      }),
      CreateLambdaTask([this](Task *internal) {
        this->executeProbeSide(internal);
      }),
      CreateLambdaTask([this] {
        this->finalize();
      })));
}

void FKPKScanJoinAggregateOperator::executeBuildSidePKScan(Task *ctx) {
  build_relation_.forEachBlockPiece(
      FKPKScanJoinAggregateOperator::kBatchSize,
      [&](auto block) {
    ctx->spawnLambdaTask([this, block] {
      std::unique_ptr<TupleIdSequence> filter(
          build_filter_predicate_->getAllMatches(*block, nullptr /* filter */));

      const std::size_t num_tuples = filter->getNumTuples();
      if (num_tuples == 0) {
        return;
      }

      DCHECK_EQ(1u, num_tuples);
      DCHECK(build_attribute_literal_ == nullptr);

      InvokeOnColumnAccessorMakeShared(
          block->createColumnAccessor(build_attribute_),
          [&](auto accessor) -> void {
        this->build_attribute_literal_ =
            std::make_shared<std::uint64_t>(accessor->at(filter->front()));
      });

      for (std::size_t i = 0; i < build_aggregate_expressions_.size(); ++i) {
        const Scalar *scalar = build_aggregate_expressions_[i];
        auto sum = scalar->accumulate(*block, filter.get());
        DCHECK(sum);
        build_sums_base_[i] = *sum;
      }
    });
  });
}

void FKPKScanJoinAggregateOperator::executeProbeSide(Task *ctx) {
  if (!build_attribute_literal_) {
    return;
  }

  auto probe_attribute = std::make_unique<ScalarAttribute>(
      &probe_relation_.getAttribute(probe_attribute_));

  auto build_literal = std::make_unique<ScalarLiteral>(
      *build_attribute_literal_, UInt64Type::Instance());

  join_predicate_ = std::make_unique<Comparison>(
      ComparisonType::kEqual, probe_attribute.release(), build_literal.release());

  probe_relation_.forEachBlockPiece(
      FKPKScanJoinAggregateOperator::kBatchSize,
      [this, ctx](auto block) {
    ctx->spawnLambdaTask([this, block](Task *internal) {
      this->executeProbeSideBlock(
          internal, std::static_pointer_cast<const StorageBlock>(block));
    });
  });
}

void FKPKScanJoinAggregateOperator::executeProbeSideBlock(
    Task *ctx, const std::shared_ptr<const StorageBlock> &block) {
  std::unique_ptr<TupleIdSequence> join_filter(
      join_predicate_->getAllMatches(*block, nullptr /* filter */));

  if (probe_filter_predicate_) {
    join_filter.reset(
        probe_filter_predicate_->getAllMatches(*block, join_filter.release()));
  }

  std::shared_ptr<TupleIdSequence> filter(join_filter.release());

  const std::size_t num_tuples = filter->getNumTuples();
  if (num_tuples == 0) {
    return;
  }

  ctx->spawnLambdaTask([this, block, filter] {
    for (std::size_t i = 0; i < probe_aggregate_expressions_.size(); ++i) {
      const Scalar *scalar = this->probe_aggregate_expressions_[i];
      auto sum = scalar->accumulate(*block, filter.get());
      DCHECK(*sum);

      this->probe_sums_[i]->fetch_add(*sum, std::memory_order_relaxed);
    }
  });

  for (std::size_t i = 0; i < build_aggregate_expressions_.size(); ++i) {
    build_sums_[i]->fetch_add(build_sums_base_[i] * num_tuples,
                              std::memory_order_relaxed);
  }

  is_null_.store(false, std::memory_order_relaxed);
}

void FKPKScanJoinAggregateOperator::finalize() {
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
