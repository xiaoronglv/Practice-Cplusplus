#include "operators/SortMergeJoinOperator.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "operators/expressions/Predicate.hpp"
#include "scheduler/Task.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/RowStoreBlock.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/ContainerUtil.hpp"
#include "utility/Range.hpp"
#include "utility/RowStoreBlockSorter.hpp"

#include "glog/logging.h"

namespace project {

namespace {

template <typename CppTypeList, typename RowStoreBlockType, typename WriteSequence>
inline void WriteTupleIntoRowStoreBlock(
    const std::vector<std::unique_ptr<ColumnAccessor>> &accessors,
    const std::size_t row,
    const tuple_id tid,
    RowStoreBlockType *row_block) {
  // Writings will be inlined with compiler optimization.
  WriteSequence::ForEach([&](auto typelist) -> void {
    constexpr std::size_t cid = decltype(typelist)::head::value;
    using CppType = typename CppTypeList::template at<cid>;
    using ColumnAccessorType = ColumnStoreColumnAccessor<CppType>;

    const ColumnAccessorType *accessor =
        static_cast<const ColumnAccessorType*>(accessors[cid].get());

    row_block->template writeValue<cid>(row, accessor->at(tid));
  });
}

template <typename Accessor, typename CppType>
inline std::size_t LocateStart(const Accessor &accessor,
                               const std::size_t start,
                               const std::size_t end,
                               const CppType value) {
  std::size_t pos = start + 1;
  while (pos < end && accessor->at(pos) < value) {
    ++pos;
  }
  return pos;
}

template <typename Accessor, typename CppType>
inline std::size_t LocateEnd(const Accessor &accessor,
                             const std::size_t start,
                             const std::size_t end,
                             const CppType value) {
  std::size_t pos = start + 1;
  while (pos < end && accessor->at(pos) <= value) {
    ++pos;
  }
  return pos;
}

template <typename Accessor, typename CppType>
inline std::size_t BinarySearchLowerBound(const Accessor &accessor,
                                          const std::size_t start,
                                          const std::size_t end,
                                          const CppType value) {
  DCHECK_LE(start, end);
  std::size_t lo = start;
  std::size_t hi = end;
  while (lo < hi) {
    const std::size_t mid = (lo + hi) >> 1;
    const CppType mid_value = accessor->at(mid);
    if (mid_value < value) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  DCHECK_EQ(lo, hi);
  return lo;
}

}  // namespace

SortMergeJoinOperator::SortMergeJoinOperator(
    const std::size_t query_id,
    const Relation &probe_relation,
    const Relation &build_relation,
    Relation *output_relation,
    const attribute_id probe_attribute,
    const attribute_id build_attribute,
    std::vector<attribute_id> &&project_attributes,
    std::vector<JoinSide> &&project_attribute_sides,
    std::unique_ptr<Predicate> &&probe_filter_predicate,
    std::unique_ptr<Predicate> &&build_filter_predicate)
    : RelationalOperator(query_id),
      num_output_attributes_(project_attributes.size()),
      output_relation_(output_relation),
      probe_context_(probe_relation, std::move(probe_filter_predicate)),
      build_context_(build_relation, std::move(build_filter_predicate)) {
  probe_context_.attributes.emplace_back(probe_attribute);
  probe_context_.output_column_indexes.emplace_back(-1);

  build_context_.attributes.emplace_back(build_attribute);
  build_context_.output_column_indexes.emplace_back(-1);

  for (std::size_t i = 0; i < project_attributes.size(); ++i) {
    const JoinSide side = project_attribute_sides[i];
    const attribute_id attribute = project_attributes[i];

    if (side == JoinSide::kProbe) {
      if (attribute == probe_attribute) {
        probe_context_.output_column_indexes[0] = i;
      } else {
        probe_context_.attributes.emplace_back(attribute);
        probe_context_.output_column_indexes.emplace_back(i);
      }
    } else {
      DCHECK(side == JoinSide::kBuild);
      if (attribute == build_attribute) {
        build_context_.output_column_indexes[0] = i;
      } else {
        build_context_.attributes.emplace_back(attribute);
        build_context_.output_column_indexes.emplace_back(i);
      }
    }
  }
}

void SortMergeJoinOperator::execute(Task *ctx) {
  if (probe_context_.relation.getNumTuples() == 0 ||
      build_context_.relation.getNumTuples() == 0) {
    return;
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        this->collectSort(internal, &probe_context_);
        this->collectSort(internal, &build_context_);
      }),
      CreateLambdaTask([&](Task *internal) {
        this->mergeHelper(internal);
      })));
}

void SortMergeJoinOperator::collectSort(Task *ctx, JoinContext *context) {
  auto output_blocks = MakeSharedVector<std::unique_ptr<RowStoreBlock>>();

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, context, output_blocks](Task *internal) {
        std::vector<std::shared_ptr<const StorageBlock>> input_blocks;
        context->relation.forEachBlockPiece(
            SortMergeJoinOperator::kBatchSize,
            [&](auto block) {
          input_blocks.emplace_back(block);
        });

        const std::size_t num_blocks = input_blocks.size();
        output_blocks->resize(num_blocks);

        for (std::size_t i = 0; i < num_blocks; ++i) {
          const auto input_block = input_blocks[i];
          const auto output_block = &output_blocks->at(i);
          internal->spawnLambdaTask([this, context, input_block, output_block] {
            this->collectBlock(*input_block, output_block, context);
          });
        }
      }),
      CreateLambdaTask([this, context, output_blocks](Task *internal) {
        const std::size_t num_blocks = output_blocks->size();
        std::vector<std::size_t> offsets(num_blocks);
        std::size_t num_tuples = 0;
        for (std::size_t i = 0; i < num_blocks; ++i) {
          offsets[i] = num_tuples;
          num_tuples += output_blocks->at(i)->getNumTuples();
        }

        DCHECK(!output_blocks->empty());
        context->buffer.reset(output_blocks->front()->newInstance(num_tuples));

        for (std::size_t i = 0; i < num_blocks; ++i) {
          const std::size_t offset = offsets[i];
          const auto output_block = output_blocks->at(i).get();
          internal->spawnLambdaTask([this, context, offset, output_block] {
            output_block->copyInto(context->buffer.get(), offset);
          });
        }
      }),
      CreateLambdaTask([this, output_blocks, context](Task *internal) {
        // NOTE(robin-team): The temporary output_blocks should be captured by
        // value to guarantee its lifetime though it is not used in this lambda.
        this->sort(internal, context);
      })));
}

void SortMergeJoinOperator::collectBlock(const StorageBlock &block,
                                         std::unique_ptr<RowStoreBlock> *output,
                                         JoinContext *context) {
  if (context->attributes.size() > 3) {
    LOG(FATAL) << "Cannot not handle more than 3 attriubtes";
  }

  std::unique_ptr<TupleIdSequence> filter;
  if (context->filter_predicate != nullptr) {
    filter.reset(context->filter_predicate->getAllMatches(block, nullptr));
  }

  std::vector<std::unique_ptr<ColumnAccessor>> accessors;
  for (const attribute_id id : context->attributes) {
    accessors.emplace_back(block.createColumnAccessor(id));
  }

  InvokeOnTypeVectorForCppTypes(
      GetTypes(*context),
      [&](auto typelist) -> void {
    using CppTypeList = decltype(typelist);

    using RowStoreBlockType =
        typename CppTypeList::template bind_to<RowStoreBlockImpl>;

    using WriteSequence = typename meta::MakeSequence<CppTypeList::length>
                                       ::template bind_to<meta::TypeList>;

    std::unique_ptr<RowStoreBlockType> row_block;

    if (filter != nullptr) {
      const std::size_t num_tuples = filter->getNumTuples();
      row_block = std::make_unique<RowStoreBlockType>(num_tuples);

      std::size_t row = 0;
      for (const tuple_id tid : *filter) {
        WriteTupleIntoRowStoreBlock<
            CppTypeList, RowStoreBlockType, WriteSequence>(
                accessors, row, tid, row_block.get());
        ++row;
      }
    } else {
      const std::size_t num_tuples = block.getNumTuples();
      row_block = std::make_unique<RowStoreBlockType>(num_tuples);

      std::size_t row = 0;
      for (std::size_t i = 0; i < num_tuples; ++i) {
        WriteTupleIntoRowStoreBlock<
            CppTypeList, RowStoreBlockType, WriteSequence>(
                accessors, row, i, row_block.get());
        ++row;
      }
    }

    output->reset(row_block.release());
  });
}

void SortMergeJoinOperator::sort(Task *ctx, JoinContext *context) {
  SortRowStoreBlock(ctx, GetTypes(*context), context->buffer.get());
}

template <typename ProbeCppType, typename BuildCppType>
void SortMergeJoinOperator::merge(const Range &range) {
  if (range.size() == 0) {
    return;
  }

  const RowStoreBlock &probe_block = *probe_context_.buffer;
  const RowStoreBlock &build_block = *build_context_.buffer;

  using ProbeAccessorType = RowStoreColumnAccessor<ProbeCppType>;
  using BuildAccessorType = RowStoreColumnAccessor<BuildCppType>;

  std::unique_ptr<ProbeAccessorType> probe_accessor(
      static_cast<ProbeAccessorType*>(probe_block.createColumnAccessor(0)));
  std::unique_ptr<BuildAccessorType> build_accessor(
      static_cast<BuildAccessorType*>(build_block.createColumnAccessor(0)));

  OrderedTupleIdSequence probe_tuples;
  OrderedTupleIdSequence build_tuples;

  probe_tuples.reserve(range.size() * 2);
  build_tuples.reserve(range.size() * 2);

  const std::size_t probe_end = range.end();
  const std::size_t build_end = build_accessor->getNumTuples();

  std::size_t probe_start = range.begin();
  ProbeCppType probe_value = probe_accessor->at(probe_start);

  std::size_t build_start =
      BinarySearchLowerBound(build_accessor, 0, build_end, probe_value);

  if (build_start >= build_end) {
    return;
  }
  BuildCppType build_value = build_accessor->at(build_start);

  while (true) {
    if (probe_value < build_value) {
      probe_start =
          LocateStart(probe_accessor, probe_start, probe_end, build_value);
      if (probe_start >= probe_end) {
        break;
      }
      probe_value = probe_accessor->at(probe_start);
    } else if (probe_value > build_value) {
      build_start =
          LocateStart(build_accessor, build_start, build_end, probe_value);
      if (build_start >= build_end) {
        break;
      }
      build_value = build_accessor->at(build_start);
    } else {
      const std::size_t probe_next =
          LocateEnd(probe_accessor, probe_start, probe_end, probe_value);
      const std::size_t build_next =
          LocateEnd(build_accessor, build_start, build_end, build_value);

      for (std::size_t i = probe_start; i < probe_next; ++i) {
        for (std::size_t j = build_start; j < build_next; ++j) {
          probe_tuples.emplace_back(i);
          build_tuples.emplace_back(j);
        }
      }

      if (probe_next >= probe_end) {
        break;
      }
      if (build_next >= build_end) {
        break;
      }

      probe_start = probe_next;
      build_start = build_next;
      probe_value = probe_accessor->at(probe_start);
      build_value = build_accessor->at(build_start);
    }
  }

  const std::size_t num_output_tuples = probe_tuples.size();
  DCHECK_EQ(num_output_tuples, build_tuples.size());

  std::vector<std::unique_ptr<ColumnVector>> output_columns(num_output_attributes_);

  materializeHelper(probe_context_, probe_tuples, &output_columns);
  materializeHelper(build_context_, build_tuples, &output_columns);

  auto output_block = std::make_unique<ColumnStoreBlock>();
  for (auto &output_column : output_columns) {
    output_block->addColumn(output_column.release());
  }

  output_relation_->addBlock(output_block.release());
}

template <bool first_null, typename RowStoreBlockType>
void SortMergeJoinOperator::materialize(
    const RowStoreBlockType &block,
    const OrderedTupleIdSequence &tuples,
    const std::vector<attribute_id> &attributes,
    std::vector<std::unique_ptr<ColumnVector>*> *output_columns) {
  using ZeroBasedSequence =
      typename meta::MakeSequence<RowStoreBlockType::ColumnTypeList::length>
                   ::template bind_to<meta::TypeList>;

  using OneBasedSequence = typename ZeroBasedSequence::template skip<1>;

  using WriteSequence =
      std::conditional_t<first_null, OneBasedSequence, ZeroBasedSequence>;

  WriteSequence::ForEach([&](auto typelist) {
    constexpr std::size_t cid = decltype(typelist)::head::value;
    using CppType = typename RowStoreBlockType::ColumnTypeList::template at<cid>;
    *((*output_columns)[cid]) =
        std::make_unique<ColumnVectorImpl<CppType>>(tuples.size());
  });

  for (const tuple_id tid : tuples) {
    WriteSequence::ForEach([&](auto typelist) {
      constexpr std::size_t cid = decltype(typelist)::head::value;
      using CppType = typename RowStoreBlockType::ColumnTypeList::template at<cid>;
      auto *column = static_cast<ColumnVectorImpl<CppType>*>((*output_columns)[cid]->get());

      column->appendValue(block.template at<cid>(tid));
    });
  }
}

void SortMergeJoinOperator::materializeHelper(
    const JoinContext &context,
    const OrderedTupleIdSequence &tuples,
    std::vector<std::unique_ptr<ColumnVector>> *output_columns) {
  std::vector<std::unique_ptr<ColumnVector>*> target_output_columns;
  for (const int index : context.output_column_indexes) {
    if (index < 0) {
      DCHECK(target_output_columns.empty());
      target_output_columns.emplace_back(nullptr);
    } else {
      target_output_columns.emplace_back(&output_columns->at(index));
    }
  }
  DCHECK_EQ(context.attributes.size(), target_output_columns.size());

  if (target_output_columns.size() == 1 &&
      target_output_columns.front() == nullptr) {
    return;
  }

  InvokeOnTypeVectorForCppTypes(
      GetTypes(context),
      [&](auto typelist) -> void {
    using RowStoreBlockType =
        typename decltype(typelist)::template bind_to<RowStoreBlockImpl>;

    const RowStoreBlockType &block =
        static_cast<const RowStoreBlockType&>(*context.buffer);

    if (target_output_columns.front() == nullptr) {
      this->materialize<true>(block,
                              tuples,
                              context.attributes,
                              &target_output_columns);
    } else {
      this->materialize<false>(block,
                              tuples,
                              context.attributes,
                              &target_output_columns);
    }
  });
}

void SortMergeJoinOperator::mergeHelper(Task *ctx) {
  if (probe_context_.buffer->getNumTuples() == 0 ||
      build_context_.buffer->getNumTuples() == 0) {
    return;
  }

  const Type &probe_type =
      probe_context_.relation.getAttributeType(probe_context_.attributes.front());

  const Type &build_type =
      build_context_.relation.getAttributeType(build_context_.attributes.front());

  const RangeSplitter splitter =
      RangeSplitter::CreateWithPartitionLength(
          0, probe_context_.buffer->getNumTuples(), kBatchSize);

  for (const Range range : splitter) {
    ctx->spawnLambdaTask([&, range](Task *internal) {
      InvokeOnTypeIDsForCppTypes(
          probe_type.getTypeID(), build_type.getTypeID(),
          [&](auto typelist) -> void {
        using ProbeCppType = typename decltype(typelist)::template at<0>;
        using BuildCppType = typename decltype(typelist)::template at<1>;

        this->merge<ProbeCppType, BuildCppType>(range);
      });
    });
  }
}

std::vector<const Type*> SortMergeJoinOperator::GetTypes(
    const JoinContext &context) {
  std::vector<const Type*> types;
  for (const attribute_id id : context.attributes) {
    types.emplace_back(&context.relation.getAttributeType(id));
  }
  return types;
}

}  // namespace project
