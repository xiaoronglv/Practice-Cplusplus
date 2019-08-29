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

#ifndef ARRAY_TABLE_DISTINCT_H
#define ARRAY_TABLE_DISTINCT_H

#include "Batch.h"
#include "../basis/TaskScheduler.h"
#include "../basis/Utilities.h"

#include <atomic>

// #define PRINT_PARTITION_HITS_AND_OVERFLOWS
#ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
    #include <iostream>
#endif

namespace query_processor {

    template<uint32_t TABLE_PARTITION_SIZE>
    class ArrayTableDistinct{

        private:
            // uint8_t instead of bool since array has to be byte addressable
            std::vector<uint8_t> _array;

            uint32_t _buildColumnPipelineIdKey;
            uint64_t _hashTableHitCount = 0;

            uint64_t _min;
            uint64_t _max;

            HTState _state = UNINIZIALIZED;
            std::mutex _stateMutex;

        public:

            ArrayTableDistinct() {
            }

            void initialize(uint32_t buildColumnPipelineIdKey, uint64_t min, uint64_t max){
                // ensures that the partitions are initialized only once, lock the mutex and change state at the end of this function
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != UNINIZIALIZED){
                //     throw std::runtime_error("Hash table initialization state error");
                // }
                // set members
                _buildColumnPipelineIdKey = buildColumnPipelineIdKey;
                _min = min;
                _max = max;

                // resize vector of bools
                _array.resize(_max - _min + 1, 0);

                // set state
                _state=PARTITIONS_INI;
            }


            void pushBatch(Batch<TABLE_PARTITION_SIZE>* batch, uint32_t ){
                // ensures that the array is initialized
                // if(_state != PARTITIONS_INI){
                //     throw std::runtime_error("Hash table push state error");
                // }
                // get the pointers to both columns
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartitionKey = batch->getColumnPartition(_buildColumnPipelineIdKey);
                // run over each row in the batch
                for(uint32_t batchRowId=0; batchRowId < batch->getCurrentSize(); batchRowId++){
                    // check if row is still valid
                    if(batch->isRowValid(batchRowId)){
                        // set the corresponding bool
                        _array[partitionColumnPartitionKey->getEntry(batchRowId) - _min] = 1;
                    }
                }
            }

            void earlyPartition(){
                // lock state
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != PARTITIONS_INI){
                //     throw std::runtime_error("Hash table partition state error 1");
                // }
                // set state
                _state=PARTITIONED;
            }

            class ATInputBuildTask : public basis::TaskBase {
                private:
                    Batch<TABLE_PARTITION_SIZE>* _batch;
                    ArrayTableDistinct<TABLE_PARTITION_SIZE>* _hashTable;

                public:
                    ATInputBuildTask(Batch<TABLE_PARTITION_SIZE>* batch, ArrayTableDistinct<TABLE_PARTITION_SIZE>* hashTable)
                    : TaskBase(basis::OLAP_TASK_PRIO, batch->getNumaNode()), _batch(batch), _hashTable(hashTable){
                    }

                    void execute(){
                        _hashTable->pushBatch(_batch, basis::Worker::getId());
                    }
            };

            void latePartition(std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>>& buildSideBatches){
                // ensures that the array is initialized
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != PARTITIONS_INI){
                //     throw std::runtime_error("Hash table partition state error 2");
                // }
                // partition, i.e. write each tuple's key and row id into the corresponding 'ArrayTableDistinct'
                basis::TaskGroup partitionTaskGroup;
                // for each batch, create a build task
                for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& batch : buildSideBatches){
                    partitionTaskGroup.addTask(std::shared_ptr<basis::TaskBase>( new(batch->getNumaNode()) ATInputBuildTask(batch.get(),this) ));
                }
                partitionTaskGroup.execute();
                // set state
                _state=PARTITIONED;
            }

            void build(){
                // ensures that the bitmap is partitioned
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != PARTITIONED){
                //     throw std::runtime_error("Hash table build state error");
                // }
                // set state
                _state=BUILD;
            }

            void check(uint64_t tupleCount){
                // ensures that the bitmap is build
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != BUILD){
                //     throw std::runtime_error("Hash table check state error");
                // }
                // run over the partitions and calculate stats
                for(uint32_t value=0; value<_array.size(); value++){
                    _hashTableHitCount += _array[value];
                }
                #ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
                    std::cout << "_hashTableHitCount: " << _hashTableHitCount << std::endl;
                #endif
                if(tupleCount != _hashTableHitCount){
                    // throw std::runtime_error("Detected inconsistency while building hash table: " + std::to_string(tupleCount) + " != " +
                    //     std::to_string(_hashTableHitCount));
                }
                // set state
                _state=CHECKED;
            }

            bool probe(uint64_t key) {
                // if(this->_state != CHECKED){
                //     throw std::runtime_error("Array Hash Table not yet build");
                // }
                return key >= _min && key <= _max && _array[key - _min];
            }

            void swap(ArrayTableDistinct<TABLE_PARTITION_SIZE>& other){
                // TODO potential deadlock
                std::unique_lock<std::mutex> uLock(_stateMutex);
                std::unique_lock<std::mutex> uLockOther(other._stateMutex);
                std::swap(_array, other._array);
                std::swap(_buildColumnPipelineIdKey, other._buildColumnPipelineIdKey);
                std::swap(_hashTableHitCount, other._hashTableHitCount);
                std::swap(_min, other._min);
                std::swap(_max, other._max);
                std::swap(_state, other._state);
            }

            uint64_t getHashTableHitCount() const {
                // if(_state != CHECKED){
                //     throw std::runtime_error("Hash table stat error 2");
                // }
                return _hashTableHitCount;
            }
    };
}

#endif
