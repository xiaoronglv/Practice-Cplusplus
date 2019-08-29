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

#ifndef CONCISE_ARRAY_TABLE_ONE_PAYLOAD_DISTINCT_H
#define CONCISE_ARRAY_TABLE_ONE_PAYLOAD_DISTINCT_H

#include "Batch.h"
#include "PrefixArray.h"
#include "../basis/TaskScheduler.h"
#include "../basis/Utilities.h"

// #define PRINT_PARTITION_HITS_AND_OVERFLOWS
#ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
    #include <iostream>
#endif

namespace query_processor {


    //
    // CATEntry = uint64_t _value; (no need to store the key, since the key is the index)


    template<uint32_t TABLE_PARTITION_SIZE>
    class ConciseArrayTableOnePayloadDistinct{

        private:
            // uint8_t instead of bool since bitmap has to be byte addressable
            std::vector<uint8_t> _bitmap;
            std::vector<uint64_t> _array;

            uint64_t _hashTableHitCount = 0;

            uint32_t _buildColumnPipelineIdKey;
            uint32_t _buildColumnPipelineIdValue;

            uint64_t _min;
            uint64_t _max;

            HTState _state = UNINIZIALIZED;
            std::mutex _stateMutex;

        public:
            ConciseArrayTableOnePayloadDistinct(){
            }

            void initializePartitions(uint32_t buildColumnPipelineIdKey, uint32_t buildColumnPipelineIdValue, uint64_t min, uint64_t max){
                // ensures that the partitions are initialized only once, lock the mutex and change state at the end of this function
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != UNINIZIALIZED){
                //     throw std::runtime_error("Hash table initialization state error");
                // }
                // set members
                _buildColumnPipelineIdKey = buildColumnPipelineIdKey;
                _buildColumnPipelineIdValue = buildColumnPipelineIdValue;
                _min = min;
                _max = max;

                // resize vector of bools and the array
                _bitmap.resize(_max - _min + 1, 0);
                _array.resize(_max - _min + 1, 0);

                // set state
                _state=PARTITIONS_INI;
            }


            void pushBatch(Batch<TABLE_PARTITION_SIZE>* batch, uint32_t){
                // ensures that the bitmap is initialized
                // if(_state != PARTITIONS_INI){
                //     throw std::runtime_error("Hash table push state error");
                // }
                // get the pointers to both columns
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartitionKey = batch->getColumnPartition(_buildColumnPipelineIdKey);
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartitionValue = batch->getColumnPartition(_buildColumnPipelineIdValue);
                // run over each row in the batch
                for(uint32_t batchRowId=0; batchRowId < batch->getCurrentSize(); batchRowId++){
                    // check if row is still valid
                    if(batch->isRowValid(batchRowId)){
                        // get the key, set the bit in the bitmap that indicates that the key is used and store the value in the array
                        uint64_t index = partitionColumnPartitionKey->getEntry(batchRowId) - _min;
                        _bitmap[index] = 1;
                        _array[index] = partitionColumnPartitionValue->getEntry(batchRowId);
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

            class CATInputPartitionTask : public basis::TaskBase {
                private:
                    Batch<TABLE_PARTITION_SIZE>* _batch;
                    ConciseArrayTableOnePayloadDistinct<TABLE_PARTITION_SIZE>* _hashTable;

                public:
                    CATInputPartitionTask(Batch<TABLE_PARTITION_SIZE>* batch, ConciseArrayTableOnePayloadDistinct<TABLE_PARTITION_SIZE>* hashTable)
                    : TaskBase(basis::OLAP_TASK_PRIO, batch->getNumaNode()), _batch(batch), _hashTable(hashTable){
                    }

                    void execute(){
                        _hashTable->pushBatch(_batch, basis::Worker::getId());
                    }
            };

            void latePartition(std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>>& buildSideBatches){
                // ensures that the bitmap is initialized
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != PARTITIONS_INI){
                //     throw std::runtime_error("Hash table partition state error 2");
                // }
                // partition, i.e. write each tuple's key and row id into the corresponding 'ConciseArrayTableOnePayloadDistinct'
                basis::TaskGroup partitionTaskGroup;
                // for each batch, create a partion task
                for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& batch : buildSideBatches){
                    partitionTaskGroup.addTask(std::shared_ptr<basis::TaskBase>( new(batch->getNumaNode()) CATInputPartitionTask(batch.get(),this) ));
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
                for(uint32_t value=0; value<_bitmap.size(); value++){
                    _hashTableHitCount += _bitmap[value];
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
                return key >= _min && key <= _max && _bitmap[key - _min];
            }

            uint64_t getProbeResult(uint64_t key) {
                return _array[key - _min];
            }


            void swap(ConciseArrayTableOnePayloadDistinct<TABLE_PARTITION_SIZE>& other){
                // TODO potential deadlock
                std::unique_lock<std::mutex> uLock(_stateMutex);
                std::unique_lock<std::mutex> uLockOther(other._stateMutex);
                std::swap(_bitmap, other._bitmap);
                std::swap(_array, other._array);
                std::swap(_hashTableHitCount, other._hashTableHitCount);
                std::swap(_buildColumnPipelineIdKey, other._buildColumnPipelineIdKey);
                std::swap(_buildColumnPipelineIdValue, other._buildColumnPipelineIdValue);
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
