#include "operators/HashJoinOperator.hpp"

#include <cstddef>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "operators/expressions/Predicate.hpp"
#include "scheduler/Task.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/meta/Common.hpp"
#include "utility/meta/TypeList.hpp"

#include "glog/logging.h"

namespace project {

BasicHashJoinOperator::BasicHashJoinOperator(
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
      build_context_(build_relation, std::move(build_filter_predicate)),
      build_side_num_tuples_(0) {
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

void BasicHashJoinOperator::execute(Task *ctx) {
  if (probe_context_.relation.getNumTuples() == 0 ||
      build_context_.relation.getNumTuples() == 0) {
    return;
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        this->buildHash(internal);
      }),
      CreateLambdaTask([&](Task *internal) {
        if (this->build_side_num_tuples_ == 0) {
          return;
        }
        this->probeHash(internal);
      })));
}

template <typename CppTypeList, typename TupleIdContainer>
void BasicHashJoinOperator::buildHashInternal(
    const std::vector<std::unique_ptr<ColumnAccessor>> &accessors,
    const TupleIdContainer &build_input_tuple_ids) {
  using HashTableType = typename CppTypeList::template bind_to<HashTableImpl>;
  using Tuple = typename HashTableType::Tuple;

  using ValueSequence = typename meta::MakeSequence<CppTypeList::length>
                                     ::template bind_to<meta::TypeList>
                                     ::template skip<1>;

  if (hash_table_ == nullptr) {
    hash_table_ = std::make_unique<HashTableType>();

    DCHECK(hash_filter_ == nullptr);
    hash_filter_ = std::make_unique<HashFilter>(build_side_num_tuples_ * 64u);
  }

  using KeyAccessorType = ColumnStoreColumnAccessor<typename CppTypeList::head>;
  const KeyAccessorType *key_accessor =
      static_cast<const KeyAccessorType*>(accessors.front().get());

  auto *table = static_cast<HashTableType&>(*hash_table_).getTableMutable();

  for (const auto tid : build_input_tuple_ids) {
    Tuple tuple;
    ValueSequence::ForEach([&](auto typelist) -> void {
      constexpr std::size_t cid = decltype(typelist)::head::value;
      using ValueType = typename CppTypeList::template at<cid>;
      using ColumnAccessorType = ColumnStoreColumnAccessor<ValueType>;

      const ColumnAccessorType *value_accessor =
          static_cast<const ColumnAccessorType*>(accessors[cid].get());

      std::get<cid-1>(tuple) = value_accessor->at(tid);
    });

    const auto key = key_accessor->at(tid);
    table->emplace(key, tuple);
    hash_filter_->insert(key);
  }
}

void BasicHashJoinOperator::buildHashBlock(const std::size_t index) {
  const StorageBlock &block = *build_blocks_[index];

  const TupleIdSequence *filter = nullptr;
  if (build_context_.filter_predicate != nullptr) {
    DCHECK_LT(index, build_filters_.size());
    filter = build_filters_[index].get();
    if (filter == nullptr) {
      return;
    }
  }

  std::vector<std::unique_ptr<ColumnAccessor>> accessors;
  for (const attribute_id id : build_context_.attributes) {
    accessors.emplace_back(block.createColumnAccessor(id));
  }

  InvokeOnTypeVectorForCppTypes(GetTypes(build_context_),
      [&](auto typelist) -> void {
    using CppTypeList = decltype(typelist);
    if (filter != nullptr) {
      this->buildHashInternal<CppTypeList>(accessors, *filter);
    } else {
      const auto tuples = Range(0, block.getNumTuples()).getGenerator();
      this->buildHashInternal<CppTypeList>(accessors, tuples);
    }
  });
}

void BasicHashJoinOperator::createBuildContext(Task *ctx) {
  build_context_.relation.forEachBlockPiece(
      BasicHashJoinOperator::kBatchSize,
      [&](auto block) -> void {
    build_blocks_.emplace_back(block);
  });

  if (build_context_.filter_predicate != nullptr) {
    const std::size_t num_blocks = build_blocks_.size();
    build_filters_.resize(num_blocks);
    for (std::size_t i = 0; i < num_blocks; ++i) {
      ctx->spawnLambdaTask([this, i] {
        std::unique_ptr<TupleIdSequence> filter(
            this->build_context_.filter_predicate->getAllMatches(
                *this->build_blocks_[i], nullptr));
        const std::size_t num_tuples = filter->getNumTuples();
        if (num_tuples > 0) {
          this->build_side_num_tuples_.fetch_add(num_tuples,
                                                 std::memory_order_relaxed);
          this->build_filters_[i] = std::move(filter);
        }
      });
    }
  } else {
    build_side_num_tuples_.fetch_add(build_context_.relation.getNumTuples(),
                                     std::memory_order_relaxed);
  }
}

void BasicHashJoinOperator::buildHash(Task *ctx) {
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&](Task *internal) {
        this->createBuildContext(internal);
      }),
      CreateLambdaTask([&](Task *internal) {
        if (this->build_side_num_tuples_ == 0) {
          return;
        }
        for (std::size_t i = 0; i < build_blocks_.size(); ++i) {
          this->buildHashBlock(i);
        }
      })));
}

template <bool first_null, typename CppTypeList,
          typename ProbeAccessor, typename TupleIdContainer>
void BasicHashJoinOperator::probeHashInternal(
    const ProbeAccessor &probe_accessor,
    const TupleIdContainer &probe_input_tuple_ids,
    OrderedTupleIdSequence *probe_output_tuples,
    std::vector<std::unique_ptr<ColumnVector>*> *build_output_columns) {
  using WriteSequence = typename meta::MakeSequence<CppTypeList::length>
                                     ::template bind_to<meta::TypeList>
                                     ::template skip<1>;

  using HashTableType = typename CppTypeList::template bind_to<HashTableImpl>;
  using Bucket = typename HashTableType::Bucket;

  const auto &table = static_cast<HashTableType&>(*hash_table_).getTable();

  DCHECK(hash_filter_ != nullptr);

  std::vector<const Bucket*> build_output_buckets;
  build_output_buckets.reserve(probe_accessor.getNumTuples());

  for (const auto tid : probe_input_tuple_ids) {
    const auto value = probe_accessor.at(tid);
    if (!hash_filter_->contains(value)) {
      continue;
    }

    const auto it_range = table.equal_range(value);
    for (auto it = it_range.first; it != it_range.second; ++it) {
      probe_output_tuples->emplace_back(tid);
      build_output_buckets.emplace_back(&(*it));
    }
  }

  using FirstColumnVectorType = ColumnVectorImpl<typename CppTypeList::head>;

  if (!first_null) {
    *((*build_output_columns)[0]) =
        std::make_unique<FirstColumnVectorType>(build_output_buckets.size());
  }

  WriteSequence::ForEach([&](auto typelist) {
    constexpr std::size_t cid = decltype(typelist)::head::value;
    using CppType = typename CppTypeList::template at<cid>;
    *((*build_output_columns)[cid]) =
        std::make_unique<ColumnVectorImpl<CppType>>(build_output_buckets.size());
  });

  for (const Bucket *bucket : build_output_buckets) {
    if (!first_null) {
      auto *column = static_cast<FirstColumnVectorType*>(
          (*build_output_columns)[0]->get());

      column->appendValue(bucket->first);
    }

    WriteSequence::ForEach([&](auto typelist) {
      constexpr std::size_t cid = decltype(typelist)::head::value;
      using CppType = typename CppTypeList::template at<cid>;
      using ColumnVectorType = ColumnVectorImpl<CppType>;
      auto *column = static_cast<ColumnVectorType*>(
          (*build_output_columns)[cid]->get());

      column->appendValue(std::get<cid-1>(bucket->second));
    });
  }
}

void BasicHashJoinOperator::probeHashBlock(const StorageBlock &block) {
  std::vector<std::unique_ptr<ColumnVector>> output_columns(num_output_attributes_);

  std::vector<std::unique_ptr<ColumnVector>*> build_output_columns;
  for (const int index : build_context_.output_column_indexes) {
    if (index < 0) {
      DCHECK(build_output_columns.empty());
      build_output_columns.emplace_back(nullptr);
    } else {
      build_output_columns.emplace_back(&output_columns[index]);
    }
  }

  std::unique_ptr<TupleIdSequence> probe_filter;
  if (probe_context_.filter_predicate != nullptr) {
    probe_filter.reset(
        probe_context_.filter_predicate->getAllMatches(block, nullptr));
  }

  OrderedTupleIdSequence probe_tuples;
  probe_tuples.reserve(block.getNumTuples());

  InvokeOnColumnAccessorMakeShared(
      block.createColumnAccessor(probe_context_.attributes.front()),
      [&](auto probe_accessor) -> void {
    InvokeOnTypeVectorForCppTypes(GetTypes(build_context_),
        [&](auto typelist) -> void {
      using CppTypeList = decltype(typelist);
      if (probe_filter != nullptr) {
        if (build_output_columns.front() == nullptr) {
          this->probeHashInternal<true, CppTypeList>(
              *probe_accessor, *probe_filter, &probe_tuples, &build_output_columns);
        } else {
          this->probeHashInternal<false, CppTypeList>(
              *probe_accessor, *probe_filter, &probe_tuples, &build_output_columns);
        }
      } else {
        const auto input_tuples = Range(0, block.getNumTuples()).getGenerator();
        if (build_output_columns.front() == nullptr) {
          this->probeHashInternal<true, CppTypeList>(
              *probe_accessor, input_tuples, &probe_tuples, &build_output_columns);
        } else {
          this->probeHashInternal<false, CppTypeList>(
              *probe_accessor, input_tuples, &probe_tuples, &build_output_columns);
        }
      }
    });
  });

  if (probe_tuples.empty()) {
    return;
  }

  for (std::size_t i = 0; i < probe_context_.attributes.size(); ++i) {
    const int index = probe_context_.output_column_indexes[i];
    if (index < 0) {
      continue;
    }

    InvokeOnColumnAccessorMakeShared(
        block.createColumnAccessor(probe_context_.attributes[i]),
        [&](auto accessor) -> void {
      using Accessor = std::remove_pointer_t<decltype(accessor.get())>;
      using CppType = typename Accessor::ValueType;
      using OutputColumnVector = ColumnVectorImpl<CppType>;

      std::unique_ptr<OutputColumnVector> output_column =
          std::make_unique<OutputColumnVector>(probe_tuples.size());

      for (const tuple_id tid : probe_tuples) {
        output_column->appendValue(accessor->at(tid));
      }

      output_columns[index] = std::move(output_column);
    });
  }

  auto output_block = std::make_unique<ColumnStoreBlock>();
  for (auto &column : output_columns) {
    DCHECK(column != nullptr);
    output_block->addColumn(column.release());
  }
  output_relation_->addBlock(output_block.release());
}

void BasicHashJoinOperator::probeHash(Task *ctx) {
  probe_context_.relation.forEachBlockPiece(
      BasicHashJoinOperator::kBatchSize,
      [&](auto block) -> void {
    ctx->spawnLambdaTask([this, block] {
      this->probeHashBlock(*block);
    });
  });
}

std::vector<const Type*> BasicHashJoinOperator::GetTypes(
    const JoinContext &context) {
  std::vector<const Type*> types;

  for (const attribute_id id : context.attributes) {
    types.emplace_back(&context.relation.getAttributeType(id));
  }
  return types;
}

}  // namespace project
