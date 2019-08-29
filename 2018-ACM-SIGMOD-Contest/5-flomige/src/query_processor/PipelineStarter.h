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

#ifndef PIPELINE_STARTER_H
#define PIPELINE_STARTER_H

#include "../basis/TaskScheduler.h"
#include "../database/Table.h"
#include "PipelineBreaker.h"

namespace query_processor {

    // forward declaration
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinOperatorBase;

    // starter base class contains first operator member as well as the standard task classes to execute and flush the pipeline
    template<uint32_t TABLE_PARTITION_SIZE>
    class PipelineStarterBase {

        public:
            // standard pipeline excution task taking a batch and pushing it to the first operator i.e. recursively through the pipe
            // TODO maybe move to 'TaskCreatingPipelineStarter' since thats the point where they are used!?
            class PipelineExecutionTask : public basis::TaskBase {
                private:
                    std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> _batch;
                    std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> _firstOperator;

                public:
                    PipelineExecutionTask(basis::TaskPriority priority, uint32_t numaNode, std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch,
                        std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> firstOperator)
                    : basis::TaskBase(priority, numaNode), _batch(batch), _firstOperator(firstOperator){
                    }

                    void execute(){
                        // push it through the pipe
                        _firstOperator->push(_batch, basis::Worker::getId());
                    }

            };

            // simple task that just pushes the specified batch of a worker in an operator to the next operator
            class BatchFlushTask : public basis::TaskBase{
                private:
                    FlushingOperator<TABLE_PARTITION_SIZE>* _flushOperator;
                    uint32_t _workerIdToFlush;

                public:
                    BatchFlushTask(basis::TaskPriority priority, uint32_t numaNode, FlushingOperator<TABLE_PARTITION_SIZE>* flushOperator, uint32_t workerIdToFlush)
                    : TaskBase(priority, numaNode), _flushOperator(flushOperator), _workerIdToFlush(workerIdToFlush){
                    }

                    void execute(){
                        _flushOperator->flushBatch(_workerIdToFlush, basis::Worker::getId());
                    }

            };

        protected:
            std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> _firstOperator=nullptr;
            std::atomic<uint64_t> _validRowCount; // TODO this is not maintained by all derived starters
            RuntimeStatistics* _runtimeStatistics;

        public:
            PipelineStarterBase(RuntimeStatistics* runtimeStatistics)
            : _validRowCount(0), _runtimeStatistics(runtimeStatistics) {
            }

            virtual ~PipelineStarterBase(){
            }

            void setFirstOperator(std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> firstOperator){
                // if(_firstOperator != nullptr){
                //     throw std::runtime_error("Next operator already set");
                // }
                _firstOperator = firstOperator;
            }

            std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> getFirstOperator() {
                return _firstOperator;
            }



            // TODO update comments
            // checks if the target pipe of an instant temp operator is ready for execution
            // initially invoked by 'Pipeline::addExecutionTasks' and recursively invoked by 'Pipeline::preExecutionPreparations', searches for
            // 'InstantTempOperator's starting from '_firstOperator', when found invokes 'InstantTempOperator::targetPipePreExecutionPreparations' which then may invoke
            // 'Pipeline::preExecutionPreparations'
            // TODO rename to 'pipelinePreExecutionSteps'
            void pipelinePreExecutionPreparations() const {
                // run over the operators starting from this starters '_firstOperator' until the pipeline breaker
                std::shared_ptr<HashJoinOperatorBase<TABLE_PARTITION_SIZE>> hashJoinOperator
                 = OperatorBase<TABLE_PARTITION_SIZE>::findNextHashJoinOperator(_firstOperator ,true);
                while(hashJoinOperator != nullptr){
                    // invoke build the hash table for each 'HashJoinOperatorBase' we find
                    hashJoinOperator->buildHashTable();
                    hashJoinOperator = OperatorBase<TABLE_PARTITION_SIZE>::findNextHashJoinOperator(
                        std::static_pointer_cast<OperatorBase<TABLE_PARTITION_SIZE>>(hashJoinOperator),false);
                }
            }

            std::shared_ptr<basis::AsyncTaskGroup> addFlushTaskGroups(std::shared_ptr<basis::AsyncTaskGroup> previousTaskGroup){
                // run over the operators starting from this starters '_firstOperator' until the pipeline breaker
                std::shared_ptr<FlushingOperator<TABLE_PARTITION_SIZE>> flushOperator
                 = OperatorBase<TABLE_PARTITION_SIZE>::findNextFlushingOperator(_firstOperator,true);
                while(flushOperator != nullptr){
                    // create a task group with tasks that flush all batches in that operator
                    std::shared_ptr<basis::AsyncTaskGroup> flushTaskGroup = basis::AsyncTaskGroup::createInstance();
                    // create a task for each worker
                    for(uint32_t workerId=0; workerId<basis::TaskScheduler::getWorkerCount(); workerId++){
                        std::shared_ptr<basis::TaskBase> task( new(basis::TaskScheduler::getWorkersNumaNode(workerId))
                            BatchFlushTask(basis::HIGH_TASK_PRIO, basis::TaskScheduler::getWorkersNumaNode(workerId), flushOperator.get(), workerId));
                        flushTaskGroup->addTask(task);
                    }
                    // set 'flushTaskGroup' as next task group in the previous task group
                    previousTaskGroup->setNextTaskGroup(flushTaskGroup);
                    // update previous task group
                    previousTaskGroup = flushTaskGroup;
                    // find next 'FlushingOperator'
                    flushOperator = OperatorBase<TABLE_PARTITION_SIZE>::findNextFlushingOperator(
                        std::static_pointer_cast<OperatorBase<TABLE_PARTITION_SIZE>>(flushOperator),false);
                }
                // return the last task group to wait on
                return previousTaskGroup;
            }

            virtual void setRuntimeStatistics(){
                if(_runtimeStatistics != nullptr){
                    _runtimeStatistics->_trueCardinality = _validRowCount;
                }
            }

            // TODO deprecated
            void printOperatorsValidRowCount() const {
                if(_firstOperator!=nullptr){
                    _firstOperator->printValidRowCount();
                }
            }
    };


    // base class for all starters that create tasks i.e. almost all except the instant temp starter
    template<uint32_t TABLE_PARTITION_SIZE>
    class TaskCreatingPipelineStarter : public PipelineStarterBase<TABLE_PARTITION_SIZE> {
        public:
            TaskCreatingPipelineStarter(RuntimeStatistics* runtimeStatistics)
            : PipelineStarterBase<TABLE_PARTITION_SIZE>(runtimeStatistics) {
            }

            virtual ~TaskCreatingPipelineStarter(){
            }

            // pure virtual function that should create an execution task per batch and add this task to the corresponding task group
            virtual void addExecutionTasks(std::shared_ptr<basis::AsyncTaskGroup> executionTaskGroup) = 0;
    };


    // a pipeline starter that starts on each kind of pipeline breaker ('PipelineBreakerBase') e.g. starts on a default breaker or a hash join build breaker
    // TODO maybe change name since any starter is now creating tasks during flush
    template<uint32_t TABLE_PARTITION_SIZE>
    class BreakerStarter : public TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE> {
        private:
            std::shared_ptr<PipelineBreakerBase<TABLE_PARTITION_SIZE>> _breaker;
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _pipelineColumns;

            void splitBatch(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> inputBatch, uint32_t tupleThreshold, std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>>& target) {
                // ensure some constraints
                // if(tupleThreshold == 0){
                //     throw std::runtime_error("Inconsitency while splitting batch 1");
                // }
                // if(inputBatch->getValidRowCount() <= tupleThreshold){
                //     throw std::runtime_error("Inconsitency while splitting batch 2");
                // }
                // keep some tuples in the original batch
                uint32_t batchRowId = 0;
                uint32_t counter = tupleThreshold;
                while(counter > 0){
                    if(inputBatch->isRowValid(batchRowId)){
                        counter--;
                    }
                    batchRowId++;
                }
                // remove tuples from the original batch and copy them to new batches
                std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> currentBatch(new(inputBatch->getNumaNode()) Batch<TABLE_PARTITION_SIZE>(_pipelineColumns, inputBatch->getNumaNode()));
                uint32_t tuplesInCurrentBatch = 0;
                while(batchRowId < inputBatch->getCurrentSize()){
                    if(inputBatch->isRowValid(batchRowId)){
                        if(tuplesInCurrentBatch >= tupleThreshold){
                            target.push_back(currentBatch);
                            currentBatch = std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>( new(inputBatch->getNumaNode()) Batch<TABLE_PARTITION_SIZE>(_pipelineColumns, inputBatch->getNumaNode()) );
                            tuplesInCurrentBatch=0;
                        }
                        // get a new row id 'tupleThreshold' is per definition never larger than the batch size
                        uint32_t newRowId;
                        if(!currentBatch->addTupleIfPossible(newRowId)){
                            throw std::runtime_error("Inconsitency while splitting batch 3");
                        }
                        tuplesInCurrentBatch++;
                        // copy the original tuple to the new tuple
                        inputBatch->copyTupleTo(batchRowId, _pipelineColumns, currentBatch.get(), newRowId);
                        // invalidate the row in the original batch
                        inputBatch->invalidateRow(batchRowId);
                    }
                    batchRowId++;
                }
                // add 'currentBatch' if it is not empty
                if(currentBatch->getValidRowCount() > 0){
                    target.push_back(currentBatch);
                }
                // also add the original batch
                target.push_back(inputBatch);
            }

        public:
            BreakerStarter(
                std::shared_ptr<PipelineBreakerBase<TABLE_PARTITION_SIZE>> breaker,
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& pipelineColumns,
                RuntimeStatistics* runtimeStatistics
            ) : TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE>(runtimeStatistics), _breaker(breaker), _pipelineColumns(pipelineColumns) {
            }

            void addExecutionTasks(std::shared_ptr<basis::AsyncTaskGroup> executionTaskGroup) {
                // consume the target batches
                std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>> targetBatches;
                _breaker->consumeBatches(targetBatches);

                // determine the maximum number of tuples per batch to prevent imbalances especially for small tuple numbers
                // TODO replace '40' by number of workers
                // uint32_t tupleThreshold;
                // if(_breaker->getValidRowCount() < 40) {
                //     tupleThreshold = 1;
                // }
                // else if(targetBatches.size() < 40) {
                //     tupleThreshold = _breaker->getValidRowCount()/40;
                // }
                // else{
                //     tupleThreshold = (_breaker->getValidRowCount() / targetBatches.size()) * 2;
                // }

                // create and add tasks
                for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& batch : targetBatches){
                    // if(batch->getValidRowCount() <= tupleThreshold){
                        std::shared_ptr<basis::TaskBase> task(
                            new(batch->getNumaNode()) typename PipelineStarterBase<TABLE_PARTITION_SIZE>::PipelineExecutionTask(
                                basis::MEDIUM_TASK_PRIO, batch->getNumaNode(), batch, PipelineStarterBase<TABLE_PARTITION_SIZE>::_firstOperator));
                        executionTaskGroup->addTask(task);
                    // }
                    // else{
                    //     // uint64_t before = batch->getValidRowCount();
                    //     // uint64_t after = 0;
                    //     std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>> splitedBatches;
                    //     splitBatch(batch, tupleThreshold, splitedBatches);
                    //     for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& splittedBatch : splitedBatches){
                    //         // after += splittedBatch->getValidRowCount();
                    //         std::shared_ptr<basis::TaskBase> task(
                    //             new(splittedBatch->getNumaNode()) typename PipelineStarterBase<TABLE_PARTITION_SIZE>::PipelineExecutionTask(
                    //                 basis::MEDIUM_TASK_PRIO, splittedBatch->getNumaNode(), splittedBatch, PipelineStarterBase<TABLE_PARTITION_SIZE>::_firstOperator));
                    //         executionTaskGroup->addTask(task);
                    //     }
                    //     // if(before != after){
                    //     //     throw std::runtime_error("Tuple mismatch after splitting batch (" + std::to_string(before) + "->" + std::to_string(after) + ")");
                    //     // }
                    // }
                }
            }
    };


    // base class for all starters that start on a base table, the distiction into base table starters speeds up initial projections since they are the only starters
    // that create batches from table partitions and not just forward or copy them
    template<uint32_t TABLE_PARTITION_SIZE>
    class BaseTableStarter : public TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE> {
        protected:
            // vector with ids of columns/column partitions that build a batch, initially those are all columns of a base table
            // is updated in case this pipe's first operator is a projection
            std::vector<uint32_t> _initialColumnIds;  // _initialColumnIds[pipelineColumnId]

        public:

            BaseTableStarter(std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> baseTable, RuntimeStatistics* runtimeStatistics)
            : TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE>(runtimeStatistics) {
                // run over the base table columns
                uint32_t columnsSize = baseTable->getColumns().size();
                for(uint32_t columnId=0; columnId < columnsSize; columnId++){
                    // add its column id to initial column ids
                    _initialColumnIds.push_back(columnId);
                }
            }

            BaseTableStarter(
                std::vector<uint32_t>& initialColumnIds,
                RuntimeStatistics* runtimeStatistics
            ) : TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE>(runtimeStatistics), _initialColumnIds(initialColumnIds) {
            }

            ~BaseTableStarter(){
            }

            bool isFirstOperatorSet(){
                return PipelineStarterBase<TABLE_PARTITION_SIZE>::_firstOperator!=nullptr;
            }

            void clearInitialColumnIds(){
                _initialColumnIds.clear();
            }

            void addInitialColumnId(uint32_t initialColumnId){
                _initialColumnIds.push_back(initialColumnId);
            }
    };


    // a base table starter that does a 'full table scan' i.e. taking all table partitions, create batches, determine tuple visibilty and start pushing
    // the batches through the pipe
    template<uint32_t TABLE_PARTITION_SIZE>
    class FullTableStarter : public BaseTableStarter<TABLE_PARTITION_SIZE> {
        private:
            std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> _baseTable;

            // task for creating a batch from a table partition and pushing it through the pipe
            class FullTablePipelineExecutionTask : public basis::TaskBase {
                private:
                    const std::vector<uint32_t>& _initialColumnIds;
                    const database::TablePartition<TABLE_PARTITION_SIZE>* _tablePartition;
                    std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> _firstOperator;
                    std::atomic<uint64_t>& _validRowCount;

                public:
                    FullTablePipelineExecutionTask(
                        basis::TaskPriority priority, uint32_t numaNode, const std::vector<uint32_t>& initialColumnIds,
                        const database::TablePartition<TABLE_PARTITION_SIZE>* tablePartition,
                        std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> firstOperator, std::atomic<uint64_t>& validRowCount)
                    : basis::TaskBase(priority, numaNode),
                        _initialColumnIds(initialColumnIds),
                        _tablePartition(tablePartition),
                        _firstOperator(firstOperator),
                        _validRowCount(validRowCount){
                    }

                    // implementation of task interface 'execute()', creates batches out of table partitions, determines visibility,
                    // and pushes batch to the first operator i.e. the pipe
                    void execute(){
                        // create a batch based on the table partition, including visibility check
                        std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch(
                            new(_tablePartition->getNumaNode()) Batch<TABLE_PARTITION_SIZE>(_tablePartition, _initialColumnIds)
                        );
                        // push batch through the pipe
                        if(batch->getValidRowCount() > 0){
                            // update true cardinality
                            _validRowCount += batch->getValidRowCount();
                            // push batch to first operator
                            _firstOperator->push(batch, basis::Worker::getId());
                        }
                    }
            };

        public:
            FullTableStarter(
                std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> baseTable,
                std::vector<uint32_t>& initialColumnIds,
                RuntimeStatistics* runtimeStatistics)
            : BaseTableStarter<TABLE_PARTITION_SIZE>(initialColumnIds, runtimeStatistics), _baseTable(baseTable){
            }
            // takes all table partitions from the base table and creates for each one a 'PipelineExecutionTask'
            void addExecutionTasks(std::shared_ptr<basis::AsyncTaskGroup> executionTaskGroup){
                // fetch table partitions from the base table
                std::vector<database::TablePartition<TABLE_PARTITION_SIZE>*> tableBasePartitions;
                _baseTable->getTablePartitions(tableBasePartitions);
                // create tasks and add them to task group
                for(database::TablePartition<TABLE_PARTITION_SIZE>* tablePartition : tableBasePartitions){
                    std::shared_ptr<basis::TaskBase> task(new(tablePartition->getNumaNode()) typename FullTableStarter<TABLE_PARTITION_SIZE>::FullTablePipelineExecutionTask(
                        basis::MEDIUM_TASK_PRIO, tablePartition->getNumaNode(), BaseTableStarter<TABLE_PARTITION_SIZE>::_initialColumnIds,
                        tablePartition, PipelineStarterBase<TABLE_PARTITION_SIZE>::_firstOperator,
                        PipelineStarterBase<TABLE_PARTITION_SIZE>::_validRowCount));
                    executionTaskGroup->addTask(task);
                }
            }
    };

}

#endif
