// SIGMOD Programming Contest 2018 Submission
// Copyright (C) 2018  Florian Wolf, Michael Brendle, Georgios Psaropoulos
//
// This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with this program; if not, see
// <http://www.gnu.org/licenses/>.

#ifndef HASH_JOIN_OPERATOR_CAT_ONE_PAYLOAD_DISTINCT_H
#define HASH_JOIN_OPERATOR_CAT_ONE_PAYLOAD_DISTINCT_H

#include "ConciseArrayTableOnePayloadDistinct.h"
#include "HashJoinOperatorBase.h"

// #define PRINT_BUILD_COLUMNS_NAMES
// #define PRINT_DEALLOC_MESSAGE
#include <iostream>



namespace query_processor {

    //
    template <uint32_t TABLE_PARTITION_SIZE>
    class HashJoinOperatorCATOnePayloadDistinct : public HashJoinOperatorBase<TABLE_PARTITION_SIZE> {

        private:
            // determine if this is the last join for the query
            bool _isLastJoin = false;

        protected:
            ColumnIdMappingsContainer _columnIdMappings;
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _buildPipeColumns;
            std::vector<uint32_t> _probeColumnPipelineIds;
            ConciseArrayTableOnePayloadDistinct<TABLE_PARTITION_SIZE> _hashTable;
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _probeSideColumnsOnly;
            uint32_t _buildBreakerColumnId; // maps the columns id of the build pipe to the columns ids of the probe pipe
            uint32_t _probePipeColumnId;
            std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> _buildSidePayloadColumn;

        public:

            HashJoinOperatorCATOnePayloadDistinct(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, // since this is a flushing operator we need this information for base class
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly, // necessary in expanding joins when we have to duplicate tuples
                ColumnIdMappingsContainer& columnIdMappings, // maps the columns ids of the build pipe to the columns ids of the probe pipe
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns, // column pointer needed in the probe/push function to add empty column partitions to the pushed batches
                const std::vector<uint32_t>& probeColumnPipelineIds,  // indicates the column we probe on
                RuntimeStatistics* runtimeStatistics
            ) : HashJoinOperatorBase<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics),
                _probeColumnPipelineIds(probeColumnPipelineIds){
                _probeSideColumnsOnly.swap(probeSideColumnsOnly);
                _columnIdMappings.swap(columnIdMappings);
                _buildPipeColumns.swap(buildPipeColumns);
                // determine if it is the last join in the query
                uint32_t projectionCount = 0;
                uint32_t operatorCount = 0;
                for (NameColumnStruct<TABLE_PARTITION_SIZE>& column : this->_columnsContainer.getColumns()) {
                    projectionCount += column._projectionCount;
                    operatorCount += column._operatorCount;
                }
                if (projectionCount == operatorCount) {
                    _isLastJoin = true;
                }
                // determine the new probe column id and the old build breaker column id
                // TODO do it already in the pipeline
                std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& columns = _buildPipeColumns.getColumns();
                for(uint32_t oldColumnId = 0; oldColumnId < columns.size(); ++oldColumnId) {
                    auto it = _columnIdMappings.getMapping().find(oldColumnId);
                    if (it != _columnIdMappings.getMapping().end()) {
                        _buildSidePayloadColumn = columns[oldColumnId]._column;
                        _buildBreakerColumnId = oldColumnId;
                        _probePipeColumnId = it->second;
                        break;
                    }
                }
            }

            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){ // a.k.a. probe
                // add column partition for the build side entries to the batch
                batch->addNewColumnPartition(_buildSidePayloadColumn);
                auto* tempPartitions = batch->getTemporaryColumnPartitions().data();
                auto* probePipeColumnTempPartition = tempPartitions[_probePipeColumnId];
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartition = batch->getColumnPartition(_probeColumnPipelineIds[0]);
                // run over the tuples in the batch
                for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                    // check if row is still valid
                    if(batch->isRowValid(batchRowId)){
                        // cast the column partition to get the key
                        uint64_t key = partitionColumnPartition->getEntry(batchRowId);
                        // probe the key
                        if (_hashTable.probe(key)) {
                            // copy the result into the new partition
                            probePipeColumnTempPartition->setEntry(batchRowId, _hashTable.getProbeResult(key));
                        } else {
                            // the key is not in the hash table
                            batch->invalidateRow(batchRowId);
                        }
                    }
                }
                // maintain statistics and push the batch to the next operator
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                FlushingOperator<TABLE_PARTITION_SIZE>::pushNextCheckDensity(batch, workerId, _isLastJoin);
            }
    };



    //
    template <uint32_t TABLE_PARTITION_SIZE>
    class HashJoinLateOperatorCATOnePayloadDistinct : public HashJoinOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE> {
        private:
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> _buildBreaker;
            std::vector<uint32_t> _buildColumnPipelineIds;
            std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>> _buildSideBatches;

        public:
            HashJoinLateOperatorCATOnePayloadDistinct(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, // since this is a flushing operator we need this information for base class
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly, // necessary in expanding joins when we have to duplicate tuples
                ColumnIdMappingsContainer& columnIdMappings, // maps the columns ids of the build pipe to the columns ids of the probe pipe
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns, // column pointer needed in the probe/push function to add empty column partitions to the pushed batches
                const std::vector<uint32_t>& probeColumnPipelineIds,  // indicates the column we probe on
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> buildBreaker, // contains the build side tuples
                const std::vector<uint32_t>& buildColumnPipelineIds, // indicates the column we build the hashmap on
                RuntimeStatistics* runtimeStatistics
            ) : HashJoinOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE>(
                    currentPipeColumns, probeSideColumnsOnly, columnIdMappings, buildPipeColumns, probeColumnPipelineIds, runtimeStatistics),
                _buildBreaker(buildBreaker),
                _buildColumnPipelineIds(buildColumnPipelineIds){
            }

            ~HashJoinLateOperatorCATOnePayloadDistinct(){
                #ifdef PRINT_DEALLOC_MESSAGE
                    std::cout << "dealloc HashJoinLateOperator" << std::endl;
                #endif
            }

            void buildHashTable(){
                // consume build breaker
                _buildBreaker->consumeBatches(_buildSideBatches);
                // determine tuple count
                uint64_t tupleCount = _buildBreaker->getValidRowCount();
                // decide how many partition we need and create them
                this->_hashTable.initializePartitions(
                    this->_buildColumnPipelineIds[0],
                    this->_buildBreakerColumnId,
                    this->_buildPipeColumns.getColumns()[this->_buildColumnPipelineIds.at(0)]._min,
                    this->_buildPipeColumns.getColumns()[this->_buildColumnPipelineIds.at(0)]._max);
                // partition the tuples (their key) in the batches into the partitions
                this->_hashTable.latePartition(_buildSideBatches);
                // now we can build the hash table
                this->_hashTable.build();
                // ensure that the hash table build was consistent and successful
                this->_hashTable.check(tupleCount);
            }
    };



    //
    template <uint32_t TABLE_PARTITION_SIZE>
    class HashJoinEarlyBreakerCATOnePayloadDistinct : public HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE> {
        private:
            std::vector<uint32_t> _buildColumnPipelineIds;
            uint32_t _buildBreakerColumnId; // payload column id
            ConciseArrayTableOnePayloadDistinct<TABLE_PARTITION_SIZE> _hashTable;

            std::mutex _isHashTableConsumedMutex;
            bool _isHashTableConsumed=false;

        public:
            HashJoinEarlyBreakerCATOnePayloadDistinct(
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns,
                std::vector<std::string>& buildColumnsNames,
                const std::vector<uint32_t>& buildColumnPipelineIds,
                uint32_t buildBreakerColumnId,
                RuntimeStatistics* runtimeStatistics
            ) : HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>(currentPipeColumns, buildColumnsNames, runtimeStatistics),
                _buildColumnPipelineIds(buildColumnPipelineIds), _buildBreakerColumnId(buildBreakerColumnId) {
                // decide how many partition we need and create them
                _hashTable.initializePartitions(
                    _buildColumnPipelineIds[0],
                    _buildBreakerColumnId,
                    this->_tempedPipeColumns.getColumns()[_buildColumnPipelineIds.at(0)]._min,
                    this->_tempedPipeColumns.getColumns()[_buildColumnPipelineIds.at(0)]._max);
            }

            ~HashJoinEarlyBreakerCATOnePayloadDistinct(){
                #ifdef PRINT_DEALLOC_MESSAGE
                    std::cout << "dealloc HashJoinEarlyBreaker - ";
                    for(const auto& col : this->_buildColumnsNames){
                        std::cout << col << " " << std::endl;
                    }
                    std::cout << std::endl;
                #endif
            }

            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                // add batch to hash table
                _hashTable.pushBatch(batch.get(), workerId);
                // just push batch to '_batches[workerId]', tuples are not copied
                DefaultBreaker<TABLE_PARTITION_SIZE>::_batches[workerId].push_back(batch);
                // operator statistics
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
            }

            // for this particular kind of join it is not necessary to copy the batches from the join breaker to the join operator since pointers to the batch
            // tuples are already in the hashmap, so we leave the batches in the breaker and just set the state
            void setBatchesConsumed(){
                // concurrency possible in parallel query execution i.e. two breaker starter using that breaker and the corresponding pipelines are executed simoultaneously
                std::unique_lock<std::mutex> uLock(PipelineBreakerBase<TABLE_PARTITION_SIZE>::_areBatchesConsumedMutex);
                // ensure that the batches of this breaker are still available and not yet consumed
                // if(PipelineBreakerBase<TABLE_PARTITION_SIZE>::_areBatchesConsumed){
                //     throw std::runtime_error("Tried to consume a pipeline breaker that was already consumed (batches)");
                // }
                PipelineBreakerBase<TABLE_PARTITION_SIZE>::_areBatchesConsumed=true;
            }

            void consumeHashTable(ConciseArrayTableOnePayloadDistinct<TABLE_PARTITION_SIZE>& target){
                std::unique_lock<std::mutex> uLock(_isHashTableConsumedMutex);
                // ensure that the hash table is not yet consumed
                // if(_isHashTableConsumed){
                //     throw std::runtime_error("Tried to consume a pipeline breaker that was already consumed (hash table)");
                // }
                // set the corresponding state in the bitmap that indicates the end on partitioning
                _hashTable.earlyPartition();
                // move the hash table to the hash join operator
                _hashTable.swap(target);
                // mark this bitmap as consumed
                _isHashTableConsumed = true;
            }

    };



    //
    template <uint32_t TABLE_PARTITION_SIZE>
    class HashJoinEarlyOperatorCATOnePayloadDistinct : public HashJoinOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE> {
        private:
            std::shared_ptr<HashJoinEarlyBreakerCATOnePayloadDistinct<TABLE_PARTITION_SIZE>> _buildBreaker;

        public:
            HashJoinEarlyOperatorCATOnePayloadDistinct(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, // since this is a flushing operator we need this information for base class
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly, // necessary in expanding joins when we have to duplicate tuples
                ColumnIdMappingsContainer& columnIdMappings, // maps the columns ids of the build pipe to the columns ids of the probe pipe
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns, // column pointer needed in the probe/push function to add empty column partitions to the pushed batches
                const std::vector<uint32_t>& probeColumnPipelineIds, // indicates the column we probe on
                std::shared_ptr<HashJoinEarlyBreakerCATOnePayloadDistinct<TABLE_PARTITION_SIZE>> buildBreaker,
                RuntimeStatistics* runtimeStatistics
            ) : HashJoinOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE>(
                    currentPipeColumns, probeSideColumnsOnly, columnIdMappings, buildPipeColumns, probeColumnPipelineIds, runtimeStatistics),
                _buildBreaker(buildBreaker){
            }

            ~HashJoinEarlyOperatorCATOnePayloadDistinct(){
                #ifdef PRINT_DEALLOC_MESSAGE
                    std::cout << "dealloc HashJoinEarlyOperator" << std::endl;
                #endif
            }

            void buildHashTable(){
                // as long as we have a pointer to '_buildBreaker', it is ok to leave them there and we don't have to copy the batches to this operator
                // pointers to the single tuples in the build breaker batches are contained in the tuple identifiers in the hash map
                _buildBreaker->setBatchesConsumed();
                // consume the hash table from the breaker
                _buildBreaker->consumeHashTable(this->_hashTable);
                // now we can build the hash table
                this->_hashTable.build();
                // get the exact tuple count
                uint64_t tupleCount = _buildBreaker->getValidRowCount();
                #ifdef PRINT_BUILD_COLUMNS_NAMES
                    std::cout << "_buildBreaker->getBuildColumnsNames():" << std::endl;
                    for(std::string buildColumnName : _buildBreaker->getBuildColumnsNames()){
                        std::cout << buildColumnName << std::endl;
                    }
                #endif
                // ensure that the hash table build was consistent and successful
                this->_hashTable.check(tupleCount);
            }

    };

}

#endif
