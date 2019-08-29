#include "operators/IndexJoinOperator.hpp"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Predicate.hpp"
#include "operators/expressions/Scalar.hpp"
#include "scheduler/Task.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "storage/IndexManager.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"

#include "glog/logging.h"

namespace project {

PrimaryKeyIndexJoinOperator::PrimaryKeyIndexJoinOperator(
    const std::size_t query_id,
    const Relation &probe_relation,
    const Relation &build_relation,
    Relation *output_relation,
    const attribute_id probe_attribute,
    const attribute_id build_attribute,
    std::vector<std::unique_ptr<Scalar>> &&project_expressions,
    std::vector<JoinSide> &&project_expression_sides,
    std::unique_ptr<Predicate> &&probe_filter_predicate)
    : RelationalOperator(query_id),
      probe_relation_(probe_relation),
      build_relation_(build_relation),
      output_relation_(output_relation),
      probe_attribute_(probe_attribute),
      build_attribute_(build_attribute),
      project_expressions_(std::move(project_expressions)),
      project_expression_sides_(std::move(project_expression_sides)),
      probe_filter_predicate_(std::move(probe_filter_predicate)) {}

void PrimaryKeyIndexJoinOperator::execute(Task *ctx) {
  DCHECK(build_relation_.getIndexManager().hasPrimaryKeyIndex(build_attribute_));

  probe_relation_.forEachBlockPiece(
      PrimaryKeyIndexJoinOperator::kBatchSize,
      [&](auto block) {
    ctx->spawnLambdaTask([this, block] {
      this->executeBlock(*block);
    });
  });
}

void PrimaryKeyIndexJoinOperator::executeBlock(
    const StorageBlock &probe_block) const {
  DCHECK_EQ(1u, build_relation_.getNumBlocks());
  const StorageBlock &build_block = build_relation_.getBlock(0);

  const Type &index_attribute_type = build_relation_.getAttributeType(build_attribute_);
  const IndexManager &index_manager = build_relation_.getIndexManager();

  OrderedTupleIdSequence probe_tuple_ids, build_tuple_ids;

  InvokeOnTypeIDForCppType(
      index_attribute_type.getTypeID(),
      [&](auto ic) -> void {
    using CppType = typename decltype(ic)::head;
    using PrimaryKeyIndexType = PrimaryKeyIndexImpl<CppType>;

    const PrimaryKeyIndexType &pk_index =
        static_cast<const PrimaryKeyIndexType&>(
            index_manager.getPrimaryKeyIndex(build_attribute_));

    InvokeOnColumnAccessorMakeShared(
        probe_block.createColumnAccessor(probe_attribute_),
        [&](auto accessor) -> void {
      if (probe_filter_predicate_ != nullptr) {
        std::unique_ptr<TupleIdSequence> filter(
            probe_filter_predicate_->getAllMatches(probe_block, nullptr));

        const std::size_t num_probe_tuples = filter->getNumTuples();
        probe_tuple_ids.reserve(num_probe_tuples);
        build_tuple_ids.reserve(num_probe_tuples);

        for (const tuple_id probe_tuple_id : *filter) {
          const tuple_id build_tuple_id = pk_index.slotAt(accessor->at(probe_tuple_id));
          if (build_tuple_id != kInvalidAttributeID) {
            probe_tuple_ids.emplace_back(probe_tuple_id);
            build_tuple_ids.emplace_back(build_tuple_id);
          }
        }
      } else {
        const std::size_t num_probe_tuples = accessor->getNumTuples();
        probe_tuple_ids.reserve(num_probe_tuples);
        build_tuple_ids.reserve(num_probe_tuples);

        for (std::size_t i = 0; i < accessor->getNumTuples(); ++i) {
          const tuple_id build_tuple_id = pk_index.slotAt(accessor->at(i));
          if (build_tuple_id != kInvalidAttributeID) {
            probe_tuple_ids.emplace_back(i);
            build_tuple_ids.emplace_back(build_tuple_id);
          }
        }
      }
    });
  });

  DCHECK_EQ(probe_tuple_ids.size(), build_tuple_ids.size());
  if (probe_tuple_ids.empty()) {
    return;
  }

  auto output_block = std::make_unique<ColumnStoreBlock>();

  DCHECK_EQ(project_expressions_.size(), project_expression_sides_.size());
  for (std::size_t i = 0; i < project_expressions_.size(); ++i) {
    const auto &scalar = project_expressions_[i];
    const JoinSide side = project_expression_sides_[i];

    if (side == JoinSide::kProbe) {
      output_block->addColumn(scalar->getAllValues(probe_block, probe_tuple_ids));
    } else {
      DCHECK(side == JoinSide::kBuild);
      output_block->addColumn(scalar->getAllValues(build_block, build_tuple_ids));
    }
  }

  output_relation_->addBlock(output_block.release());
}

}  // namespace project
