#include "operators/aggregators/BuildKeyCountVector.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "operators/utility/KeyCountVector.hpp"
#include "partition/PartitionExecutor.hpp"
#include "partition/RangeSegmentation.hpp"
#include "storage/ColumnAccessor.hpp"
#include "storage/GenericAccessor.hpp"
#include "storage/Relation.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/BitManipulation.hpp"
#include "utility/HashFilter.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"
#include "utility/ThreadSafePool.hpp"

#include "glog/logging.h"

namespace project {

namespace {

template <typename ValueType, typename CountType>
class SingleThreadKeyCountVectorBuilder : public KeyCountVectorBuilder {
 public:
  using KeyCountVectorType = KeyCountVectorImpl<CountType>;

  SingleThreadKeyCountVectorBuilder(const Range &domain,
                                    const HashFilter *lookahead_filter,
                                    std::unique_ptr<KeyCountVector> *output)
      : domain_(domain),
        lookahead_filter_(lookahead_filter),
        output_(output) {
    output_->reset(new KeyCountVectorType(domain_.begin(), domain_.size()));
  }

  void accumulate(Task *ctx,
                  const std::shared_ptr<const ColumnAccessor> &accessor,
                  const TupleIdSequence *filter) override;

  void finalize(Task *ctx) override {}

 private:
  const Range domain_;
  const HashFilter *lookahead_filter_;

  std::unique_ptr<KeyCountVector> *output_;

  DISALLOW_COPY_AND_ASSIGN(SingleThreadKeyCountVectorBuilder);
};

template <typename ValueType, typename CountType>
void SingleThreadKeyCountVectorBuilder<ValueType, CountType>::accumulate(
    Task *ctx,
    const std::shared_ptr<const ColumnAccessor> &accessor,
    const TupleIdSequence *filter) {
  KeyCountVectorType *kcv = static_cast<KeyCountVectorType*>(output_->get());
  GenericAccessor wrapper(*accessor, domain_, filter, lookahead_filter_);

  wrapper.forEach([&](const auto &value) {
    kcv->increaseCount(value);
  });
}


template <typename ValueType, typename CountType>
class TwoPhaseKeyCountVectorBuilder : public KeyCountVectorBuilder {
 public:
  using KeyCountVectorType = KeyCountVectorImpl<CountType>;

  TwoPhaseKeyCountVectorBuilder(const Range &domain,
                                const HashFilter *lookahead_filter,
                                std::unique_ptr<KeyCountVector> *output);

  void accumulate(Task *ctx,
                  const std::shared_ptr<const ColumnAccessor> &accessor,
                  const TupleIdSequence *filter) override;

  void finalize(Task *ctx) override;

 private:
  struct KeyCountVectorAllocator {
    inline KeyCountVectorType* operator()() const {
      return new KeyCountVectorType(domain);
    }
    const Range domain;
  };

  const Range domain_;
  const HashFilter *lookahead_filter_;

  ThreadSafePool<KeyCountVectorType, KeyCountVectorAllocator> kcv_pool_;

  std::unique_ptr<KeyCountVector> *output_;

  DISALLOW_COPY_AND_ASSIGN(TwoPhaseKeyCountVectorBuilder);
};

template <typename ValueType, typename CountType>
TwoPhaseKeyCountVectorBuilder<ValueType, CountType>
    ::TwoPhaseKeyCountVectorBuilder(const Range &domain,
                                    const HashFilter *lookahead_filter,
                                    std::unique_ptr<KeyCountVector> *output)
    : domain_(domain),
      lookahead_filter_(lookahead_filter),
      kcv_pool_(KeyCountVectorAllocator({domain_})),
      output_(output) {
}

template <typename ValueType, typename CountType>
void TwoPhaseKeyCountVectorBuilder<ValueType, CountType>::accumulate(
    Task *ctx,
    const std::shared_ptr<const ColumnAccessor> &accessor,
    const TupleIdSequence *filter) {
  ctx->spawnLambdaTask([this, accessor, filter] {
    std::unique_ptr<KeyCountVectorType> kcv(kcv_pool_.allocate());
    GenericAccessor wrapper(*accessor, domain_, filter, lookahead_filter_);

    wrapper.forEach([&](const auto &value) {
      kcv->increaseCount(value);
    });

    kcv_pool_.push_back(kcv.release());
  });
}

template <typename ValueType, typename CountType>
void TwoPhaseKeyCountVectorBuilder<ValueType, CountType>::finalize(Task *ctx) {
  // TODO(robin-team): May parallelize merging if it becomes bottleneck.
  KeyCountVectorType *kcv = kcv_pool_.allocate();
  output_->reset(kcv);

  kcv_pool_.forEach([&](const auto &other) {
    kcv->mergeWith(other);
  });
}


template <typename ValueType, typename CountType>
class SharedTableKeyCountVectorBuilder : public KeyCountVectorBuilder {
 public:
  using SharedTableType = KeyCountVectorImpl<CountType, true>;
  using KeyCountVectorType = KeyCountVectorImpl<CountType>;

  SharedTableKeyCountVectorBuilder(const Range &domain,
                                   const HashFilter *lookahead_filter,
                                   std::unique_ptr<KeyCountVector> *output)
      : domain_(domain),
        lookahead_filter_(lookahead_filter),
        table_(domain),
        output_(output) {}

  void accumulate(Task *ctx,
                  const std::shared_ptr<const ColumnAccessor> &accessor,
                  const TupleIdSequence *filter) override;

  void finalize(Task *ctx) override;

 private:
  const Range domain_;
  const HashFilter *lookahead_filter_;

  SharedTableType table_;
  std::unique_ptr<KeyCountVector> *output_;

  static constexpr std::size_t kFinalizeBatchSize = 100000;

  DISALLOW_COPY_AND_ASSIGN(SharedTableKeyCountVectorBuilder);
};

template <typename ValueType, typename CountType>
void SharedTableKeyCountVectorBuilder<ValueType, CountType>::accumulate(
    Task *ctx,
    const std::shared_ptr<const ColumnAccessor> &accessor,
    const TupleIdSequence *filter) {
  ctx->spawnLambdaTask([this, accessor, filter] {
    GenericAccessor wrapper(*accessor, domain_, filter, lookahead_filter_);
    wrapper.forEach([&](const auto &value) {
      table_.increaseCount(value);
    });
  });
}

template <typename ValueType, typename CountType>
void SharedTableKeyCountVectorBuilder<ValueType, CountType>::finalize(Task *ctx) {
  KeyCountVectorType *kcv =
      new KeyCountVectorType(domain_.begin(), domain_.size());
  output_->reset(kcv);

  const RangeSplitter splitter =
      RangeSplitter::CreateWithPartitionLength(domain_, kFinalizeBatchSize);
  for (const Range range : splitter) {
    ctx->spawnLambdaTask([this, range, kcv] {
      for (const std::size_t value : range.getGenerator()) {
        kcv->increaseCount(value, table_.at(value));
      }
    });
  }
}


template <typename ValueType, typename CountType>
class PartitionedKeyCountVectorBuilder : public KeyCountVectorBuilder {
 public:
  using KeyCountVectorType = KeyCountVectorImpl<CountType>;

  PartitionedKeyCountVectorBuilder(const Range &domain,
                                   const HashFilter *lookahead_filter,
                                   std::unique_ptr<KeyCountVector> *output)
      : domain_(domain),
        lookahead_filter_(lookahead_filter),
        partitioner_(RangeSegmentation(domain, CalculateSegmentWidth(domain))),
        output_(output) {}

  void accumulate(Task *ctx,
                  const std::shared_ptr<const ColumnAccessor> &accessor,
                  const TupleIdSequence *filter) override;

  void finalize(Task *ctx) override;

 private:
  inline static std::size_t CalculateSegmentWidth(const Range &domain) {
    // TODO(robin-team): Find best width.
    const int digits = MostSignificantBit(domain.size());
    return std::max(1ull, (1ull << digits) >> 6);
  }

  const Range domain_;
  const HashFilter *lookahead_filter_;

  PartitionExecutor<ValueType, RangeSegmentation> partitioner_;
  std::unique_ptr<KeyCountVector> *output_;

  DISALLOW_COPY_AND_ASSIGN(PartitionedKeyCountVectorBuilder);
};

template <typename ValueType, typename CountType>
void PartitionedKeyCountVectorBuilder<ValueType, CountType>::accumulate(
    Task *ctx,
    const std::shared_ptr<const ColumnAccessor> &accessor,
    const TupleIdSequence *filter) {
  ctx->spawnLambdaTask([this, accessor, filter] {
    GenericAccessor wrapper(*accessor, domain_, filter, lookahead_filter_);
    partitioner_.insert(wrapper);
  });
}

template <typename ValueType, typename CountType>
void PartitionedKeyCountVectorBuilder<ValueType, CountType>::finalize(Task *ctx) {
  KeyCountVectorType *kcv = new KeyCountVectorType(domain_, false);
  output_->reset(kcv);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([kcv](Task *internal) {
        kcv->initialize(internal);
      }),
      CreateLambdaTask([this, kcv](Task *internal) {
        for (std::size_t i = 0; i < partitioner_.getNumPartitions(); ++i) {
          internal->spawnLambdaTask([this, kcv, i] {
            this->partitioner_.forEach(
                i, [&](const auto &value) {
              kcv->increaseCount(value);
            });
          });
        }
      })));
}

}  // namespace

KeyCountVectorBuilder* KeyCountVectorBuilder::Create(
    const Type &type,
    const std::size_t estimated_num_tuples,
    const Range &domain,
    const std::size_t max_count,
    const HashFilter *lookahead_filter,
    std::unique_ptr<KeyCountVector> *output) {
  return InvokeOnTypeIDForCppType(
      type.getTypeID(),
      [&](auto ic) -> KeyCountVectorBuilder* {
    using ValueType = typename decltype(ic)::head;

    return InvokeOnMaxCount(
        max_count,
        [&](auto mc) -> KeyCountVectorBuilder* {
      using CountType = typename decltype(mc)::head;

      if (estimated_num_tuples <= kSingleThreadCardinalityThreshold) {
        return new SingleThreadKeyCountVectorBuilder<ValueType, CountType>(
            domain, lookahead_filter, output);
      }

      if (domain.size() <= kTwoPhaseDomainThreshold) {
        return new TwoPhaseKeyCountVectorBuilder<ValueType, CountType>(
            domain, lookahead_filter, output);
      }

      return new PartitionedKeyCountVectorBuilder<ValueType, CountType>(
          domain, lookahead_filter, output);
    });
  });
}

}  // namespace project
