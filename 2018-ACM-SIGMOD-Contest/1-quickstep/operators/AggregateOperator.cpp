#include "operators/AggregateOperator.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "operators/expressions/ExpressionSynthesizer.hpp"
#include "scheduler/Task.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/Relation.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"

namespace project {

void BasicAggregateOperator::execute(Task *ctx) {
  if (input_relation_.getNumTuples() == 0) {
    return;
  }

  SharedVector<std::atomic<std::uint64_t>> sums =
      MakeSharedVector<std::atomic<std::uint64_t>>(aggregate_expressions_.size());

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, sums](Task *ctx) {
        input_relation_.forEachBlockPiece(
            kBatchSize,
            [this, sums, ctx](auto block) {
          ctx->spawnLambdaTask([this, sums, block] {
            this->executeBlock(*block, sums);
          });
        });
      }),
      CreateLambdaTask([this, sums](Task *internal) {
        this->finalize(*sums);
      })));
}

void BasicAggregateOperator::executeBlock(
    const StorageBlock &block,
    const SharedVector<std::atomic<std::uint64_t>> &sums) {
  const std::size_t num_columns = sums->size();
  std::vector<std::uint64_t> locals(num_columns);

  const bool fused = InvokeOnLiteralComparisonPredicate(
      filter_predicate_.get(), block,
      [&](auto comparator) -> bool {
    return InvokeOnAttributeSum(
        aggregate_expressions_, &locals, block,
        [&](auto writer) -> bool {
      bool is_not_null = false;
      for (std::size_t i = 0; i < block.getNumTuples(); ++i) {
        if ((*comparator)(i)) {
          is_not_null = true;
          writer->accumulate(i);
        }
      }
      if (is_not_null) {
        this->is_null_.store(false, std::memory_order_relaxed);
      }
      return true;
    });
  });

  if (!fused) {
    executeGeneric(block, &locals);
  }

  for (std::size_t i = 0; i < locals.size(); ++i) {
    sums->at(i).fetch_add(locals[i]);
  }
}

void BasicAggregateOperator::executeGeneric(
    const StorageBlock &block,
    std::vector<std::uint64_t> *locals) {
  std::unique_ptr<TupleIdSequence> filter;
  if (filter_predicate_ != nullptr) {
    filter.reset(filter_predicate_->getAllMatches(block, nullptr));
  }

  for (std::size_t i = 0; i < aggregate_expressions_.size(); ++i) {
    auto sum = aggregate_expressions_[i]->accumulate(block, filter.get());

    if (sum == nullptr) {
      DCHECK_EQ(0u, i);
      break;
    }

    this->is_null_.store(false, std::memory_order_relaxed);
    (*locals)[i] = *sum;
  }
}

void BasicAggregateOperator::finalize(
    const std::vector<std::atomic<std::uint64_t>> &sums) const {
  if (is_null_) {
    return;
  }

  std::unique_ptr<ColumnStoreBlock> block = std::make_unique<ColumnStoreBlock>();
  for (std::size_t i = 0; i < sums.size(); ++i) {
    auto column = std::make_unique<ColumnVectorImpl<std::uint64_t>>(1u);
    column->appendValue(sums[i]);
    block->addColumn(column.release());
  }
  output_relation_->addBlock(block.release());
}

}  // namespace project
