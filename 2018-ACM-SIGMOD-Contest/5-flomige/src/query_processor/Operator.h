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

#ifndef OPERATOR_H
#define OPERATOR_H

#include "Batch.h"

#include <atomic>

namespace query_processor {

    template<uint32_t TABLE_PARTITION_SIZE>
    class PipelineBreakerBase;

    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinOperatorBase;

    template<uint32_t TABLE_PARTITION_SIZE>
    class FlushingOperator;


    struct RuntimeStatistics {
        uint64_t _trueCardinality = std::numeric_limits<uint64_t>::max();
        bool _causedPlanSwitch = false;
    };

    template<uint32_t TABLE_PARTITION_SIZE>
    class OperatorBase {

        protected:
            std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> _nextOperator=nullptr;

            // operator stats, the amount of valid rows after the operator was entirely executed,
            // afer execution, this will be set into the runtime statistics object, we have this member for performance
            // TODO this might not be maintained by all derived operators
            std::array<uint64_t, basis::NUMA_NODE_COUNT * basis::CORES_PER_NUMA> _validRowCount;

            // pointer to a runtime statistics object that was potentially created for this operator, when it is not null we use it to set captured
            // statistics after the execution of this operator so that the operator can be deallocated and we still have the runtime statistics for printing
            RuntimeStatistics* _runtimeStatistics;

        public:
            OperatorBase(RuntimeStatistics* runtimeStatistics)
            : _runtimeStatistics(runtimeStatistics) {
                _validRowCount.fill(0);
            }

            virtual ~OperatorBase(){
            }

            // TODO ensure that this is not a bottleneck
            void addUpValidRowCount(uint32_t batchValidRowCount, uint32_t workerId){ // workerId
                _validRowCount[workerId] += batchValidRowCount;
            }

            virtual void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId) = 0;

            void setNextOperator(std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> nextOperator){
                // if(_nextOperator != nullptr){
                //     throw std::runtime_error("Next operator already set");
                // }
                _nextOperator = nextOperator;
            }

            std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> getNextOperator() const {
                return _nextOperator;
            }

            // traverse the linked list to find the next operator of the specified type, consider the start operator corresponding to the flag
            // a nullptr is returned in case there is no such operator
            // template <template<uint32_t> class OperatorType>
            // static std::shared_ptr<OperatorType<TABLE_PARTITION_SIZE>> findNextOperator(std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> startOperator, bool includeStartOperator){
            //     // determine the first operator we consider
            //     std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> oper;
            //     if(includeStartOperator){
            //         oper = startOperator;
            //     }
            //     else{
            //         oper = startOperator->getNextOperator();
            //     }
            //     // run over the linked list of operator until oper is a nullptr
            //     std::shared_ptr<OperatorType<TABLE_PARTITION_SIZE>> operTyped = nullptr;
            //     while(oper != nullptr){
            //         // return when we found a desired operator
            //         operTyped = std::dynamic_pointer_cast<OperatorType<TABLE_PARTITION_SIZE>>(oper);
            //         if(operTyped != nullptr){
            //             return operTyped;
            //         }
            //         // increment
            //         oper=oper->getNextOperator();
            //     }
            //     // when we are at the end of the linked list and did not find a corresponding operator, we return a nullptr
            //     return nullptr;
            // }

            //
            static std::shared_ptr<HashJoinOperatorBase<TABLE_PARTITION_SIZE>> findNextHashJoinOperator(std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> startOperator, bool includeStartOperator){
                // determine the first operator we consider
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> oper;
                if(includeStartOperator){
                    oper = startOperator;
                }
                else{
                    oper = startOperator->getNextOperator();
                }
                // run over the linked list of operator until oper is a nullptr
                std::shared_ptr<HashJoinOperatorBase<TABLE_PARTITION_SIZE>> operTyped = nullptr;
                while(oper != nullptr){
                    // return when we found a desired operator
                    operTyped = std::dynamic_pointer_cast<HashJoinOperatorBase<TABLE_PARTITION_SIZE>>(oper);
                    if(operTyped != nullptr){
                        return operTyped;
                    }
                    // increment
                    oper=oper->getNextOperator();
                }
                // when we are at the end of the linked list and did not find a corresponding operator, we return a nullptr
                return nullptr;
            }


            static std::shared_ptr<FlushingOperator<TABLE_PARTITION_SIZE>> findNextFlushingOperator(std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> startOperator, bool includeStartOperator){
                // determine the first operator we consider
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> oper;
                if(includeStartOperator){
                    oper = startOperator;
                }
                else{
                    oper = startOperator->getNextOperator();
                }
                // run over the linked list of operator until oper is a nullptr
                std::shared_ptr<FlushingOperator<TABLE_PARTITION_SIZE>> operTyped = nullptr;
                while(oper != nullptr){
                    // return when we found a desired operator
                    operTyped = std::dynamic_pointer_cast<FlushingOperator<TABLE_PARTITION_SIZE>>(oper);
                    if(operTyped != nullptr){
                        return operTyped;
                    }
                    // increment
                    oper=oper->getNextOperator();
                }
                // when we are at the end of the linked list and did not find a corresponding operator, we return a nullptr
                return nullptr;
            }



            // for performance reason we do not update the referenced runtime statistics object while execution, but capture statitics
            // locally and use this function to set the runtime statistics after execution, this can be overloaded by child classes
            // TODO we could make the cardinality counter in the runtime statistics object an atomic and avoid this function and the '_validRowCount' member
            virtual void setRuntimeStatistics(){
                if(_runtimeStatistics != nullptr){
                    _runtimeStatistics->_trueCardinality = getValidRowCount();
                }
            }

            void printValidRowCount() const {
                std::cout << "     operator: "<< getValidRowCount() << " valid rows" << std::endl;
                if(_nextOperator!=nullptr){
                    _nextOperator->printValidRowCount();
                }
            }

            // TODO this is a bit deprecated, ideally a runtime object should be used
            uint64_t getValidRowCount() const {
                uint64_t validRowCount(0);
                for (uint32_t worker = 0; worker < basis::NUMA_NODE_COUNT * basis::CORES_PER_NUMA; ++worker) {
                    validRowCount += _validRowCount[worker];
                }
                return validRowCount;
            }
    };


    // this class basically keeps a copy of the pipeline's columns at this point, which is helpful when we have to copy tuples
    // e.g. in flushing operators or want to create a new batch and its corresponding column partitions
    template<uint32_t TABLE_PARTITION_SIZE>
    class TupleRematerializer {
        // TODO this should be an operator
        // since PipelineBreaker is also an operator so we will have a diamond inheritance in e.g. hash join build breaker
        // we cannot add these features in operator, since result breaker and buffer breaker starter won't need them

        protected:
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _columnsContainer;

        public:
            TupleRematerializer(const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns)
            : _columnsContainer(currentPipeColumns) {
            }

            virtual ~TupleRematerializer(){
            }

            const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& getColumnsContainer() const {
                return _columnsContainer;
            }

    };


    // base class for operators that consolidate batches by invoking 'pushNextCheckDensity' at the end of their 'push' implementation,
    // as a result for each worker at most one batch remains in these operators and has to be flushed after the execution by invoking 'flushBatch'
    template<uint32_t TABLE_PARTITION_SIZE>
    class FlushingOperator : public OperatorBase<TABLE_PARTITION_SIZE>, public TupleRematerializer<TABLE_PARTITION_SIZE> {
        private:
            std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>> _denseBatches;  // _denseBatches[workerId]

        protected:
            std::atomic<uint64_t> _alreadyPushedBatches;

            // create a new tuple in the dense batch, also create a new dense batch if there is no or the curretn one is full,
            // invoked for instance by 'pushNextCheckDensity' or in expanding joins
            void createNewTupleInDenseBatch(uint32_t workerId, uint32_t numaNode, BatchTupleIdentifier<TABLE_PARTITION_SIZE>& newTupleId){
                // TODO there should be a more clever strategy to determine the numa node i.e. know how many batches are allocated on which node and allocate
                //      the new batch on the node with the least batches, we also have to take into account when 'pushNextCheckDensity' drops batches
                // create a target batch if not yet exisiting
                if(_denseBatches[workerId]==nullptr){
                    _denseBatches[workerId] = std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>( new(numaNode) Batch<TABLE_PARTITION_SIZE>(TupleRematerializer<TABLE_PARTITION_SIZE>::_columnsContainer,numaNode) );
                }
                std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> currentBatch = _denseBatches[workerId];
                // reserve/add space for a new tuple, push the current batch and create a new batch if the current batch is full
                uint32_t newBatchRowId;
                if(!currentBatch->addTupleIfPossible(newBatchRowId)){
                    // push current batch
                    _alreadyPushedBatches++;
                    OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator->push(currentBatch, workerId);
                    // create a new batch
                    _denseBatches[workerId] = std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>( new(numaNode) Batch<TABLE_PARTITION_SIZE>(TupleRematerializer<TABLE_PARTITION_SIZE>::_columnsContainer,numaNode) );
                    currentBatch = _denseBatches[workerId];
                    if(!currentBatch->addTupleIfPossible(newBatchRowId)){
                        throw std::runtime_error("Unable to add row to new batch");
                    }
                }
                newTupleId = BatchTupleIdentifier<TABLE_PARTITION_SIZE>(currentBatch.get(), newBatchRowId);
            }

            // this function has to be invoked at the end of each 'FlushingOperator' to push the batch to the next operator, it checks if the batch became empty or sparse,
            // tuples of sparse batches are copied to the worker's dense batch and the original batch is discarded
            void pushNextCheckDensity(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId, bool isLastJoin){
                // check if the batch is empty, so we will not push it
                if(batch->getValidRowCount() == 0){
                    // TODO mark somewhere that we have one batch less on this numa node, see also 'createNewTupleInDenseBatch'
                    return;
                }
                // if batch is very sparse and it is not the last join i.e. the majority of rows is invalid, add its tuples to this worker's dense batch and push that dense batch if its full,
                // consolidate batches only when a certain number of batches was already pushed to enusure balanced parallelization in case we use a breaker starter later
                // TODO replace '160' with maybe 4*#cpus
                else if(batch->getValidRowCount() < 15 && _alreadyPushedBatches.load() > 160 && !isLastJoin){
                    // TODO mark somewhere that we have one batch less on this numa node, see also 'createNewTupleInDenseBatch'
                    // run over the tuples in the batch
                    for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); batchRowId++){
                        // check if row is still valid
                        if(batch->isRowValid(batchRowId)){
                            // create a new tuple in the dense batch
                            BatchTupleIdentifier<TABLE_PARTITION_SIZE> newTupleId;
                            // pass numa node just in case we have to create a new batch
                            createNewTupleInDenseBatch(workerId, batch->getNumaNode(), newTupleId);
                            // copy data
                            batch->copyTupleTo(batchRowId, TupleRematerializer<TABLE_PARTITION_SIZE>::_columnsContainer, newTupleId._batch, newTupleId._batchRowId);
                        }
                    }
                }
                // if the batch is still very dense, we push it to the next operator
                else{
                    _alreadyPushedBatches++;
                    OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator->push(batch, workerId);
                }
            }

        public:
            FlushingOperator(const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, RuntimeStatistics* runtimeStatistics)
            :   OperatorBase<TABLE_PARTITION_SIZE>(runtimeStatistics),
                TupleRematerializer<TABLE_PARTITION_SIZE>(currentPipeColumns),
                _denseBatches(basis::TaskScheduler::getWorkerCount(), nullptr),
                _alreadyPushedBatches(0){
            }

            virtual ~FlushingOperator(){
            }

            void flushBatch(uint32_t workerIdToFlush, uint32_t thisWorkerId){
                // check if we have a batch corresponding to the workerId
                if(_denseBatches[workerIdToFlush] != nullptr){
                    // push it to the next operator
                    if(_denseBatches[workerIdToFlush]->getValidRowCount() > 0){
                        _alreadyPushedBatches++;
                        OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator->push(_denseBatches[workerIdToFlush], thisWorkerId);
                    }
                    // set shared pointer to null
                    _denseBatches[workerIdToFlush] = nullptr;
                }
            }
    };

}

#endif
