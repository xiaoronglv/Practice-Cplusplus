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

#ifndef HASH_JOIN_OPERATOR_CAT_ONE_PAYLOAD_H
#define HASH_JOIN_OPERATOR_CAT_ONE_PAYLOAD_H

#include "ConciseArrayTableOnePayload.h"
#include "HashJoinOperatorBase.h"

// #define PRINT_BUILD_COLUMNS_NAMES
// #define PRINT_DEALLOC_MESSAGE
#include <iostream>



namespace query_processor {

    //
    template <uint32_t TABLE_PARTITION_SIZE>
    class HashJoinOperatorCATOnePayload : public HashJoinOperatorBase<TABLE_PARTITION_SIZE> {

        private:
            // determine if this is the last join for the query
            bool _isLastJoin = false;

        protected:
            ColumnIdMappingsContainer _columnIdMappings;
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _buildPipeColumns;
            std::vector<uint32_t> _probeColumnPipelineIds;
            ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE> _hashTable;
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _probeSideColumnsOnly;
            uint32_t _buildBreakerColumnId; // maps the columns id of the build pipe to the columns ids of the probe pipe
            uint32_t _probePipeColumnId;
            std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> _buildSidePayloadColumn;

            // column wise version
            class OverflowMaterializationTask : public basis::TaskBase {
                private:
                    Batch<TABLE_PARTITION_SIZE>* _sourceBatch; // it is guaranteed that the source batch is not deallocated before this task finished
                    std::vector<typename ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE>::OverflowResultPartition>& _overflowResultsPartition;
                    uint32_t _currentStartIndexPartition;
                    uint32_t _currentOffsetPartition;
                    PipelineColumnsContainer<TABLE_PARTITION_SIZE>& _probePipeColumns; // necessary in expanding joins when we have to duplicate tuples
                    uint32_t _buildBreakerColumnId; // maps the columns id of the build pipe to the columns ids of the probe pipe
                    uint32_t _probePipeColumnId;
                    std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> _buildSidePayloadColumn;
                    OperatorBase<TABLE_PARTITION_SIZE>* _nextOperator;

                public:
                    OverflowMaterializationTask(
                        Batch<TABLE_PARTITION_SIZE>* sourceBatch,
                        std::vector<typename ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE>::OverflowResultPartition>& overflowResultsPartition,
                        uint32_t currentStartIndexPartition,
                        uint32_t currentOffsetPartition,
                        PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probePipeColumns,
                        uint32_t buildBreakerColumnId, // maps the columns id of the build pipe to the columns ids of the probe pipe
                        uint32_t probePipeColumnId,
                        std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> buildSidePayloadColumn,
                        OperatorBase<TABLE_PARTITION_SIZE>* nextOperator
                    )
                    : basis::TaskBase(basis::OLAP_TASK_PRIO, sourceBatch->getNumaNode()),
                        _sourceBatch(sourceBatch),
                        _overflowResultsPartition(overflowResultsPartition),
                        _currentStartIndexPartition(currentStartIndexPartition),
                        _currentOffsetPartition(currentOffsetPartition),
                        _probePipeColumns(probePipeColumns),
                        _buildBreakerColumnId(buildBreakerColumnId),
                        _probePipeColumnId(probePipeColumnId),
                        _buildSidePayloadColumn(buildSidePayloadColumn),
                        _nextOperator(nextOperator) {
                    }

                    void execute(){
                        // create a new batch
                        std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> targetBatch(new(_sourceBatch->getNumaNode()) Batch<TABLE_PARTITION_SIZE>(_probePipeColumns, _sourceBatch->getNumaNode()));
                        // add column partition for the build side
                        targetBatch->addNewColumnPartition(_buildSidePayloadColumn);
                        uint32_t spaceLeft = Batch<TABLE_PARTITION_SIZE>::getMaximumSize();
                        uint32_t startIndexPartition = _currentStartIndexPartition;
                        // iterate over the overflow results from the partitions
                        // (1) copy the old results row by row and create the new batch row id
                        bool first = true;
                        while(spaceLeft > 0 && startIndexPartition < _overflowResultsPartition.size()){
                            // determine iterator to the start of current overflow result
                            uint32_t startOffset = _overflowResultsPartition[startIndexPartition]._start;
                            // apply offset, but only for the first overflow result
                            if(first){
                                  startOffset += _currentOffsetPartition;
                                  first = false;
                            }
                            // iterate over each entry in the overflow result
                            while(spaceLeft > 0 && startOffset < _overflowResultsPartition[startIndexPartition]._end){
                                  // duplicate source tuple into 'targetBatch', create a new tuple and get its id
                                  uint32_t targetBatchRowId;
                                  if(!targetBatch->addTupleIfPossible(targetBatchRowId)){
                                        throw std::runtime_error("Inconsistency while duplicating tuple in 'OverflowMaterializationTask'");
                                  }
                                  // copy the original tuple (sourceBatchRowId) to the new tuple, only the probe side entries are copied
                                  _sourceBatch->copyTupleTo(_overflowResultsPartition[startIndexPartition]._sourceBatchRowId,
                                  _probePipeColumns, targetBatch.get(), targetBatchRowId);
                                  // decrement left space in this tuple
                                  spaceLeft--;
                                  // move iterator to the next probe result tuple
                                  startOffset++;
                            }
                            startIndexPartition++;
                        }
                        // (2) fill the new column
                        auto* targetBatchPtr = targetBatch.get();
                        first = true;
                        spaceLeft = Batch<TABLE_PARTITION_SIZE>::getMaximumSize();
                        startIndexPartition = _currentStartIndexPartition;
                        uint32_t targetBatchRowId = 0;
                        auto* tempPartitions = targetBatchPtr->getTemporaryColumnPartitions().data();
                        auto* tempPartition = tempPartitions[_probePipeColumnId];
                        while(spaceLeft > 0 && startIndexPartition < _overflowResultsPartition.size()){
                            // determine iterator to the start of current overflow result
                            uint32_t startOffset = _overflowResultsPartition[startIndexPartition]._start;
                            // apply offset, but only for the first overflow result
                            if(first){
                                startOffset += _currentOffsetPartition;
                                first = false;
                            }
                            // iterate over each entry in the overflow result
                            while(spaceLeft > 0 && startOffset < _overflowResultsPartition[startIndexPartition]._end){
                                  // copy the entry from the source column partition into the new target column partition
                                  tempPartition->setEntry(targetBatchRowId, _overflowResultsPartition[startIndexPartition]._array->at(startOffset));
                                  // decrement left space in this tuple
                                  spaceLeft--;
                                  // move iterator to the next probe result tuple
                                  startOffset++;
                                  // increment target batch row id
                                  ++targetBatchRowId;
                            }
                            startIndexPartition++;
                        }
                        // push the batch to the next operator
                        _nextOperator->push(targetBatch, basis::Worker::getId());
                    }
            };

        public:

            HashJoinOperatorCATOnePayload(
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
                if (_isLastJoin) {
                    pushLastJoin(batch, workerId);
                } else {
                    pushInnerJoin(batch, workerId);
                }
            }


            void pushLastJoin(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){ // a.k.a. probe
                // add column partition for the build side entries to the batch
                batch->addNewColumnPartition(_buildSidePayloadColumn);

                // get the checksum vector of this batch
                std::vector<uint64_t>& checksums = batch->getChecksums();

                // array to determine the row count for each row after the join
                std::array<uint32_t, TABLE_PARTITION_SIZE> rowDuplicateCount;
                rowDuplicateCount.fill(0);

                // cast the column partition to get the keys
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartition = batch->getColumnPartition(_probeColumnPipelineIds[0]);

                // run over the tuples in the batch
                for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                    // check if row is still valid
                    if(batch->isRowValid(batchRowId)){
                        uint64_t key = partitionColumnPartition->getEntry(batchRowId);
                        // case 1 : the key is inside the array partitions
                        if (_hashTable.isInPartitionRange(key)) {
                            typename ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE>::MainResultPartition mainResultPartition;
                            // probe for the key
                            if (_hashTable.probe(key, mainResultPartition)) {
                                // key is found
                                // store the results of matching tuples
                                rowDuplicateCount[batchRowId] = mainResultPartition._end - mainResultPartition._start;
                                // copy the sum over all results into the checksum
                                while (mainResultPartition._start < mainResultPartition._end) {
                                    checksums[_probePipeColumnId] += mainResultPartition._array->at(mainResultPartition._start);
                                    // update the iterator
                                    ++mainResultPartition._start;
                                }
                            } else {
                                // invalidate tuple if there was no matching tuple
                                batch->invalidateRow(batchRowId);
                            }
                        }
                        // case 2: the key is outside the array partitions, i.e., not in the hash table
                        else {
                            batch->invalidateRow(batchRowId);
                        }
                    }
                }

                // run over all other projection columns and calculate the checksum
                std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& columns = this->_columnsContainer.getColumns();
                for (uint32_t columnId = 0; columnId < columns.size(); ++columnId) {
                    if (columns[columnId]._isProjectionColumn && columnId != _probePipeColumnId) {
                        // get the column partition
                        const database::ColumnPartition<TABLE_PARTITION_SIZE>* checksumColumnPartition = batch->getColumnPartition(columnId);
                        // run over the tuples in the batch
                        for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                            // check if row is still valid
                            if(batch->isRowValid(batchRowId)){
                                // update the checksum
                                checksums[columnId] += rowDuplicateCount[batchRowId] * checksumColumnPartition->getEntry(batchRowId);
                            }
                        }

                    }
                }

                // set the checksum for this batch to true
                batch->setIsChecksumUsed(true);

                // maintain statistics and push the batch to the next operator
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                FlushingOperator<TABLE_PARTITION_SIZE>::pushNextCheckDensity(batch, workerId, true);
            }


            void pushInnerJoin(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){ // a.k.a. probe
                // add column partition for the build side entries to the batch
                batch->addNewColumnPartition(_buildSidePayloadColumn);
                auto* tempPartitions = batch->getTemporaryColumnPartitions().data();
                auto* probePipeColumnTempPartition = tempPartitions[_probePipeColumnId];

                // vector for overflow results
                std::vector<typename ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE>::OverflowResultPartition> overflowResultsPartition;
                uint32_t overflowResultsCount = 0;

                // cast the column partition to get the keys
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartition = batch->getColumnPartition(_probeColumnPipelineIds[0]);

                // run over the tuples in the batch
                for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                    // check if row is still valid
                    if(batch->isRowValid(batchRowId)){
                        uint64_t key = partitionColumnPartition->getEntry(batchRowId);
                        // case 1 : the key is inside the array partitions
                        if (_hashTable.isInPartitionRange(key)) {
                            typename ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE>::MainResultPartition mainResultPartition;
                            // probe for the key
                            if (_hashTable.probe(key, mainResultPartition)) {
                                // key is found
                                // copy the first probe result into the actual tuple
                                probePipeColumnTempPartition->setEntry(batchRowId, mainResultPartition._array->at(mainResultPartition._start));
                                // update the iterator
                                ++mainResultPartition._start;

                                // in case the probe result contains more than one tuple and we have to duplicate the original tuple
                                if(mainResultPartition._start < mainResultPartition._end){
                                    // handle overflow results later
                                    // push the rest of the main result into the vector
                                    overflowResultsPartition.emplace_back(mainResultPartition._array, mainResultPartition._start, mainResultPartition._end, batchRowId);
                                    overflowResultsCount += mainResultPartition._end - mainResultPartition._start;
                                }
                            } else {
                                // invalidate tuple if there was no matching tuple
                                batch->invalidateRow(batchRowId);
                            }
                        }
                        // case 2: the key is outside the array partitions, i.e., not in the hash table
                        else {
                            batch->invalidateRow(batchRowId);
                        }
                    }
                }

                // handle overflow results, create new tasks that duplicate tuples and push batches to the next operator
                if(overflowResultsCount > 0){
                    // there are multiple cases, such as multiple small overflow results will be placed in one batch,
                    // or a very large overflow result has to placed in multiple batches

                    // create a new task group, that is executed within this task, push the original batch only when the task group is finished,
                    // otherwise the original batch could be modified in the next operator, column partition ids are modified by filters and projections
                    std::shared_ptr<basis::AsyncTaskGroup> overflowGroup = basis::AsyncTaskGroup::createInstance();
                    uint64_t numaDistributionCounter = 0;

                    // maintain the number of tuples in the current batch
                    uint32_t currentBatchSize = 0;

                    // overflow partition vector
                    // index of the current 'OverflowResult'
                    uint64_t currentStartIndexPartition = 0;
                    // current offset in the current 'OverflowResult'
                    uint32_t currentOffsetPartition = 0;

                    // handle the overflow results from the partitions
                    for(uint64_t resultIndex=0; resultIndex<overflowResultsPartition.size(); resultIndex++){
                        // maintain statistics, add up additional tuples
                        OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(overflowResultsPartition[resultIndex]._size, workerId);
                        // in case the overflow result still fits into the current batch
                        if((currentBatchSize + overflowResultsPartition[resultIndex]._size) <= Batch<TABLE_PARTITION_SIZE>::getMaximumSize()){
                            currentBatchSize += overflowResultsPartition[resultIndex]._size;
                        }
                        // in case the overflow result does not fit into the current batch
                        else{
                            // create a new task for the current batch, each task starts at its given index and offset
                            // and creates a full batch or it reaches the end of the overflow results
                            overflowGroup->addTask(std::shared_ptr<OverflowMaterializationTask>(new(numaDistributionCounter++%basis::NUMA_NODE_COUNT) OverflowMaterializationTask(
                                batch.get(),
                                overflowResultsPartition,
                                currentStartIndexPartition,
                                currentOffsetPartition,
                                _probeSideColumnsOnly,
                                _buildBreakerColumnId,
                                _probePipeColumnId,
                                _buildSidePayloadColumn,
                                OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator.get()
                            )));
                            FlushingOperator<TABLE_PARTITION_SIZE>::_alreadyPushedBatches++;
                            currentStartIndexPartition = resultIndex;
                            // calculate the number of of tuples to move by a batch size
                            uint64_t tuplesToMove = overflowResultsPartition[resultIndex]._size - (Batch<TABLE_PARTITION_SIZE>::getMaximumSize() - currentBatchSize);
                            // calculate the new offset in the overflow result
                            currentOffsetPartition = overflowResultsPartition[resultIndex]._size - tuplesToMove;
                            // currentOffsetPartition = tuplesToMove;
                            // create new task/batches until the tuples to move is smaller than the size of a batch
                            while(tuplesToMove > Batch<TABLE_PARTITION_SIZE>::getMaximumSize()){
                                // create a new task, each task starts at its given index and offset and creates a full batch or it reaches the end of the overflow results
                                overflowGroup->addTask(std::shared_ptr<OverflowMaterializationTask>(new(numaDistributionCounter++%basis::NUMA_NODE_COUNT) OverflowMaterializationTask(
                                    batch.get(),
                                    overflowResultsPartition,
                                    currentStartIndexPartition,
                                    currentOffsetPartition,
                                    _probeSideColumnsOnly,
                                    _buildBreakerColumnId,
                                    _probePipeColumnId,
                                    _buildSidePayloadColumn,
                                    OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator.get()
                                )));
                                FlushingOperator<TABLE_PARTITION_SIZE>::_alreadyPushedBatches++;
                                // decrease the number of of tuples to move by a batch size
                                tuplesToMove -= Batch<TABLE_PARTITION_SIZE>::getMaximumSize();
                                // calculate the new offset in the overflow result
                                currentOffsetPartition = overflowResultsPartition[resultIndex]._size - tuplesToMove;
                            }
                            currentBatchSize = tuplesToMove;
                        }

                    }
                    // create a final tasks
                    if(currentBatchSize > 0){
                        // create a task and add it to the overflow task group
                        overflowGroup->addTask(std::shared_ptr<OverflowMaterializationTask>(new(numaDistributionCounter++%basis::NUMA_NODE_COUNT) OverflowMaterializationTask(
                            batch.get(),
                            overflowResultsPartition,
                            currentStartIndexPartition,
                            currentOffsetPartition,
                            _probeSideColumnsOnly,
                            _buildBreakerColumnId,
                            _probePipeColumnId,
                            _buildSidePayloadColumn,
                            OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator.get()
                        )));
                        FlushingOperator<TABLE_PARTITION_SIZE>::_alreadyPushedBatches++;
                    }
                    // execute the first task group
                    overflowGroup->startExecution();
                    overflowGroup->wait();
                }

                // maintain statistics and push the batch to the next operator
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                FlushingOperator<TABLE_PARTITION_SIZE>::pushNextCheckDensity(batch, workerId, _isLastJoin);
            }
    };



    //
    template <uint32_t TABLE_PARTITION_SIZE>
    class HashJoinLateOperatorCATOnePayload : public HashJoinOperatorCATOnePayload<TABLE_PARTITION_SIZE> {
        private:
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> _buildBreaker;
            std::vector<uint32_t> _buildColumnPipelineIds;
            std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>> _buildSideBatches;

        public:
            HashJoinLateOperatorCATOnePayload(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, // since this is a flushing operator we need this information for base class
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly, // necessary in expanding joins when we have to duplicate tuples
                ColumnIdMappingsContainer& columnIdMappings, // maps the columns ids of the build pipe to the columns ids of the probe pipe
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns, // column pointer needed in the probe/push function to add empty column partitions to the pushed batches
                const std::vector<uint32_t>& probeColumnPipelineIds,  // indicates the column we probe on
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> buildBreaker, // contains the build side tuples
                const std::vector<uint32_t>& buildColumnPipelineIds, // indicates the column we build the hashmap on
                RuntimeStatistics* runtimeStatistics
            ) : HashJoinOperatorCATOnePayload<TABLE_PARTITION_SIZE>(
                    currentPipeColumns, probeSideColumnsOnly, columnIdMappings, buildPipeColumns, probeColumnPipelineIds, runtimeStatistics),
                _buildBreaker(buildBreaker),
                _buildColumnPipelineIds(buildColumnPipelineIds){
            }

            ~HashJoinLateOperatorCATOnePayload(){
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
    class HashJoinEarlyBreakerCATOnePayload : public HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE> {
        private:
            std::vector<uint32_t> _buildColumnPipelineIds;
            uint32_t _buildBreakerColumnId; // payload column id
            ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE> _hashTable;

            std::mutex _isHashTableConsumedMutex;
            bool _isHashTableConsumed=false;

        public:
            HashJoinEarlyBreakerCATOnePayload(
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

            ~HashJoinEarlyBreakerCATOnePayload(){
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

            void consumeHashTable(ConciseArrayTableOnePayload<TABLE_PARTITION_SIZE>& target){
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
    class HashJoinEarlyOperatorCATOnePayload : public HashJoinOperatorCATOnePayload<TABLE_PARTITION_SIZE> {
        private:
            std::shared_ptr<HashJoinEarlyBreakerCATOnePayload<TABLE_PARTITION_SIZE>> _buildBreaker;

        public:
            HashJoinEarlyOperatorCATOnePayload(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, // since this is a flushing operator we need this information for base class
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly, // necessary in expanding joins when we have to duplicate tuples
                ColumnIdMappingsContainer& columnIdMappings, // maps the columns ids of the build pipe to the columns ids of the probe pipe
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns, // column pointer needed in the probe/push function to add empty column partitions to the pushed batches
                const std::vector<uint32_t>& probeColumnPipelineIds, // indicates the column we probe on
                std::shared_ptr<HashJoinEarlyBreakerCATOnePayload<TABLE_PARTITION_SIZE>> buildBreaker,
                RuntimeStatistics* runtimeStatistics
            ) : HashJoinOperatorCATOnePayload<TABLE_PARTITION_SIZE>(
                    currentPipeColumns, probeSideColumnsOnly, columnIdMappings, buildPipeColumns, probeColumnPipelineIds, runtimeStatistics),
                _buildBreaker(buildBreaker){
            }

            ~HashJoinEarlyOperatorCATOnePayload(){
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
