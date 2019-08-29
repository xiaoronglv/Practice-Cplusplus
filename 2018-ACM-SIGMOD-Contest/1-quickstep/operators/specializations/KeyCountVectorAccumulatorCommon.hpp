#ifndef PROJECT_OPERATORS_SPECIALIZATIONS_KEY_COUNT_VECTOR_ACCUMULATOR_COMMON_HPP_
#define PROJECT_OPERATORS_SPECIALIZATIONS_KEY_COUNT_VECTOR_ACCUMULATOR_COMMON_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "operators/utility/KeyCountVector.hpp"
#include "operators/utility/KeyCountVectorChain.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/GenericAccessor.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "operators/expressions/Scalar.hpp"
#include "operators/utility/MultiwayJoinContext.hpp"
#include "utility/HashFilter.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {
namespace kcv {

class KeyCountVectorAccumulatorCommon {
 public:
  KeyCountVectorAccumulatorCommon(MultiwayJoinContext *context,
                                  const Range &domain,
                                  const HashFilter *lookahead_filter,
                                  std::vector<const KeyCountVector*> &&kcvs,
                                  std::atomic<bool> *is_null)
      : domain_(domain),
        lookahead_filter_(lookahead_filter),
        kcvs_(std::move(kcvs)),
        context_(context),
        is_null_(is_null) {}

  bool hasLookaheadFilter() const {
    return lookahead_filter_ != nullptr;
  }

  const std::vector<const KeyCountVector*>& getKeyCountVectors() const {
    return kcvs_;
  }

  template <bool adaptive, bool lookahead, typename CountVector>
  void accumulate(Task *ctx, const CountVector &chain);

 private:
  template <bool adaptive, bool lookahead, typename CountVector>
  void accumulateBlocks(Task *ctx, const CountVector &chain);

  template <bool adaptive, bool lookahead, typename CountVector>
  void accumulateKeyCountVector(Task *ctx, const CountVector &chain);


  const Range domain_;
  const HashFilter *lookahead_filter_;
  const std::vector<const KeyCountVector*> kcvs_;

  MultiwayJoinContext *context_;
  std::atomic<bool> *is_null_;

  static constexpr std::size_t kKeyCountVectorBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(KeyCountVectorAccumulatorCommon);
};


template <bool adaptive, bool lookahead, typename CountVector>
void KeyCountVectorAccumulatorCommon::accumulate(
    Task *ctx, const CountVector &chain) {
  if (context_->scan_blocks) {
    accumulateBlocks<adaptive, lookahead>(ctx, chain);
  } else {
    accumulateKeyCountVector<adaptive, lookahead>(ctx, chain);
  }
}

template <bool adaptive, bool lookahead, typename CountVector>
void KeyCountVectorAccumulatorCommon::accumulateBlocks(
    Task *ctx, const CountVector &chain) {
  for (std::size_t i = 0; i < context_->blocks.size(); ++i) {
    ctx->spawnLambdaTask([this, chain, i] {
      const StorageBlock &block = *context_->blocks[i];
      const std::size_t num_tuples = block.getNumTuples();

      OrderedTupleIdSequence tuples;
      std::vector<std::uint64_t> counts;

      tuples.reserve(num_tuples);
      counts.reserve(num_tuples);

      std::unique_ptr<ColumnAccessor> accessor(
          block.createColumnAccessor(context_->join_attribute));

      const TupleIdSequence *filter = nullptr;
      if (context_->filter_predicate != nullptr) {
        DCHECK_LT(i, context_->filters.size());
        DCHECK(context_->filters[i] != nullptr);
        filter = context_->filters[i].get();
      }

      GenericAccessor wrapper(*accessor, domain_, filter, lookahead_filter_);
      wrapper.forEachPair([&](const tuple_id tid, const auto &value) {
        const std::uint64_t count = chain->at(value);
        if (count != 0) {
          tuples.emplace_back(tid);
          counts.emplace_back(count);
        }
      });

      if (tuples.empty()) {
        return;
      }

      for (std::size_t i = 0; i < context_->aggregate_expressions.size(); ++i) {
        const Scalar *scalar = context_->aggregate_expressions[i];
        auto sum = scalar->accumulateMultiply(block, tuples, counts);
        DCHECK(sum != nullptr);

        context_->sums[i]->fetch_add(*sum, std::memory_order_relaxed);
      }
      is_null_->store(false, std::memory_order_relaxed);
    });
  }
}

template <bool adaptive, bool lookahead, typename CountVector>
void KeyCountVectorAccumulatorCommon::accumulateKeyCountVector(
    Task *ctx, const CountVector &chain) {
  DCHECK_EQ(1u, context_->aggregate_expressions.size());
  DCHECK_EQ(1u, context_->sums.size());

  const KeyCountVector *kcv = context_->count_vector.get();
  DCHECK(kcv != nullptr);

  const RangeSplitter splitter =
      RangeSplitter::CreateWithPartitionLength(
          kcv->getRange(), kKeyCountVectorBatchSize);

  for (const Range range : splitter) {
    ctx->spawnLambdaTask([this, kcv, chain, range] {
      std::unique_ptr<KeyCountVector> subset(kcv->createSubset(range));

      std::uint64_t sum = 0;
      bool is_null = true;

      subset->forEachPair([&](const auto &value, const auto &value_count) {
        DCHECK_NE(value_count, 0);
        const std::uint64_t other_count = chain->at(value);
        if (other_count != 0) {
          sum += other_count * value_count * value;
          is_null = false;
        }
      });

      context_->sums.front()->fetch_add(sum);

      if (!is_null) {
        is_null_->store(false, std::memory_order_relaxed);
      }
    });
  }
}

template <typename Functor>
inline auto InvokeOnKeyCountVectorsMakeSharedNA(
    const std::vector<const KeyCountVector*> &kcvs,
    const Functor &functor) {
  CHECK(kcvs.empty());
  return functor(std::make_shared<CountVectorChain<>>());
}

template <typename Functor>
inline auto InvokeOnKeyCountVectorsMakeSharedX(
    const std::vector<const KeyCountVector*> &kcvs,
    const Functor &functor) {
  CHECK_EQ(1u, kcvs.size());
  return InvokeOnKeyCountVector(
      *kcvs[0],
      [&](const auto &kcv0) {
    using CV0 = std::remove_reference_t<decltype(kcv0)>;
    using CVC = CountVectorChain<CV0>;
    return functor(std::make_shared<CVC>(&kcv0));
  });
}

template <typename Functor>
inline auto InvokeOnKeyCountVectorsMakeSharedXX(
    const std::vector<const KeyCountVector*> &kcvs,
    const Functor &functor) {
  CHECK_EQ(2u, kcvs.size());
  return InvokeOnKeyCountVector(
      *kcvs[0],
      [&](const auto &kcv0) {
    using CV0 = std::remove_reference_t<decltype(kcv0)>;
    return InvokeOnKeyCountVector(
        *kcvs[1],
        [&](const auto &kcv1) {
      using CV1 = std::remove_reference_t<decltype(kcv1)>;
      using CVC = CountVectorChain<CV0, CV1>;
      return functor(std::make_shared<CVC>(&kcv0, &kcv1));
    });
  });
}

template <typename Functor>
inline auto InvokeOnKeyCountVectorsMakeSharedXXX(
    const std::vector<const KeyCountVector*> &kcvs,
    const Functor &functor) {
  CHECK_EQ(3u, kcvs.size());
  return InvokeOnKeyCountVector(
      *kcvs[0],
      [&](const auto &kcv0) {
    using CV0 = std::remove_reference_t<decltype(kcv0)>;
    return InvokeOnKeyCountVector(
        *kcvs[1],
        [&](const auto &kcv1) {
      using CV1 = std::remove_reference_t<decltype(kcv1)>;
      return InvokeOnKeyCountVector(
          *kcvs[2],
          [&](const auto &kcv2) {
        using CV2 = std::remove_reference_t<decltype(kcv2)>;
        using CVC = CountVectorChain<CV0, CV1, CV2>;
        return functor(std::make_shared<CVC>(&kcv0, &kcv1, &kcv2));
      });
    });
  });
}

}  // namespace kcv
}  // namespace project

#endif  // PROJECT_OPERATORS_SPECIALIZATIONS_KEY_COUNT_VECTOR_ACCUMULATOR_COMMON_HPP_
