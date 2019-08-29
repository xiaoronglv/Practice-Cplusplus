#include "operators/SelectOperator.hpp"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "operators/expressions/Comparison.hpp"
#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/expressions/ScalarLiteral.hpp"
#include "storage/Attribute.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/ForeignKeyIndex.hpp"
#include "storage/IndexManager.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/Relation.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Type.hpp"
#include "utility/BitVector.hpp"

#include "glog/logging.h"

namespace project {

BasicSelectOperator::BasicSelectOperator(
    const std::size_t query_id,
    const Relation &input_relation,
    Relation *output_relation,
    std::vector<std::unique_ptr<Scalar>> &&project_expressions,
    std::unique_ptr<Predicate> &&filter_predicate)
    : RelationalOperator(query_id),
      input_relation_(input_relation),
      output_relation_(output_relation),
      project_expressions_(std::move(project_expressions)),
      filter_predicate_(std::move(filter_predicate)) {}

void BasicSelectOperator::execute(Task *ctx) {
  input_relation_.forEachBlockPiece(
      BasicSelectOperator::kBatchSize,
      [&](auto block) -> void {
    ctx->spawnLambdaTask([this, block] {
      this->executeBlock(*block);
    });
  });
}

void BasicSelectOperator::executeBlock(const StorageBlock &block) {
  std::unique_ptr<TupleIdSequence> filter;
  if (filter_predicate_ != nullptr) {
    filter.reset(filter_predicate_->getAllMatches(block, nullptr));
  }

  std::unique_ptr<ColumnStoreBlock> output_block =
      std::make_unique<ColumnStoreBlock>();

  for (const auto &scalar : project_expressions_) {
    output_block->addColumn(scalar->getAllValues(block, filter.get()));
  }

  output_relation_->addBlock(output_block.release());
}

IndexScanOperator::IndexScanOperator(
    const std::size_t query_id,
    const Relation &input_relation,
    Relation *output_relation,
    std::vector<std::unique_ptr<Scalar>> &&project_expressions,
    std::unique_ptr<Comparison> &&comparison)
    : RelationalOperator(query_id),
      input_relation_(input_relation),
      output_relation_(output_relation),
      project_expressions_(std::move(project_expressions)),
      comparison_(std::move(comparison)) {}

void IndexScanOperator::execute(Task *ctx) {
  if (input_relation_.getNumTuples() == 0) {
    return;
  }
  DCHECK_EQ(1u, input_relation_.getNumBlocks());

  DCHECK(comparison_->comparison_type() == ComparisonType::kEqual);
  DCHECK(comparison_->left().getScalarType() == ScalarType::kAttribute);
  DCHECK(comparison_->right().getScalarType() == ScalarType::kLiteral);

  const Attribute *attribute =
      static_cast<const ScalarAttribute&>(comparison_->left()).attribute();
  const std::uint64_t value =
      static_cast<const ScalarLiteral&>(comparison_->right()).value();

  const IndexManager &index_manager = input_relation_.getIndexManager();

  if (index_manager.hasPrimaryKeyIndex(attribute->id())) {
    executePrimaryKey(attribute, value);
  } else {
    DCHECK(index_manager.hasForeignKeyIndex(attribute->id()));
    executeForeignKey(attribute, value);
  }
}

void IndexScanOperator::executePrimaryKey(
    const Attribute *attribute, const std::uint64_t value) {
  const IndexManager &index_manager = input_relation_.getIndexManager();
  DCHECK(index_manager.hasPrimaryKeyIndex(attribute->id()));

  const tuple_id tid =
      index_manager.getPrimaryKeyIndex(attribute->id()).lookupVirtual(value);

  if (tid != kInvalidTupleID) {
    OrderedTupleIdSequence tuples({tid});
    input_relation_.forSingletonBlock(
        [&](const auto &block) -> void {
      auto output_block = std::make_unique<ColumnStoreBlock>();

      for (const auto &scalar : project_expressions_) {
        output_block->addColumn(scalar->getAllValues(block, tuples));
      }
      output_relation_->addBlock(output_block.release());
    });
  }
}

void IndexScanOperator::executeForeignKey(
    const Attribute *attribute, const std::uint64_t value) {
  const IndexManager &index_manager = input_relation_.getIndexManager();
  DCHECK(index_manager.hasForeignKeyIndex(attribute->id()));

  OrderedTupleIdSequence tuples;
  InvokeOnTypeIDForCppType(
      attribute->getType().getTypeID(),
      [&](auto ic) -> void {
    using CppType = typename decltype(ic)::head;
    using ForeignKeyIndexType = ForeignKeyIndexImpl<CppType>;

    const ForeignKeyIndexType &fk_index =
        static_cast<const ForeignKeyIndexType&>(
            index_manager.getForeignKeyIndex(attribute->id()));

    const auto slot = fk_index.slotAt(value);
    const std::size_t num_output_tuples = slot.second;
    if (num_output_tuples > 0) {
      tuples.reserve(num_output_tuples);
      for (std::size_t i = 0; i < num_output_tuples; ++i) {
        tuples.emplace_back(fk_index.bucketAt(slot.first + i).second);
      }
    }
  });

  if (!tuples.empty()) {
    input_relation_.forSingletonBlock(
        [&](const auto &block) -> void {
      auto output_block = std::make_unique<ColumnStoreBlock>();

      for (const auto &scalar : project_expressions_) {
        output_block->addColumn(scalar->getAllValues(block, tuples));
      }
      output_relation_->addBlock(output_block.release());
    });
  }
}

}  // namespace project
