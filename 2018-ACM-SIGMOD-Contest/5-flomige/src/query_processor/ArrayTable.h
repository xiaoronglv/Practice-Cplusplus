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

#ifndef ARRAY_TABLE_H
#define ARRAY_TABLE_H

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
    class ArrayTable{
        public:

            struct OverflowResult {
                uint32_t _size;
                uint32_t _sourceBatchRowId;
                OverflowResult(uint32_t size, uint32_t sourceBatchRowId)
                 : _size(size), _sourceBatchRowId(sourceBatchRowId) {
                }
            };

            class ArrayTablePartition : public basis::NumaAllocated {

                private:
                    uint64_t _offset;

                    std::array<std::atomic<uint32_t>, TABLE_PARTITION_SIZE> _array;

                public:
                    ArrayTablePartition(uint64_t offset)
                    :   _offset(offset) {
                        for(auto& x : _array) {
                            std::atomic_init(&x, 0u);
                        }
                    }

                    virtual ~ArrayTablePartition(){
                    }

                    void addInputPair(uint64_t key){
                        _array[key - _offset]++;
                    }

                    uint64_t getTupleCount(){
                        uint64_t tupleCount = 0;
                        for(uint32_t i = 0; i < TABLE_PARTITION_SIZE; ++i){
                            tupleCount += _array[i].load();
                        }
                        return tupleCount;
                    }

                    uint64_t probe(uint64_t key) {
                        return _array[key-_offset].load();
                    }

                    uint64_t getHashTableHitCount() {
                        return getTupleCount();
                    }
            };

        private:
            std::vector<std::shared_ptr<ArrayTablePartition>> _partitions;

            uint32_t _buildColumnPipelineIdKey;
            uint64_t _hashTableHitCount = 0;

            uint64_t _min;
            uint64_t _max;

            HTState _state = UNINIZIALIZED;
            std::mutex _stateMutex;

            // returns the partition
            uint32_t getPartition(uint64_t key) const {
                return (key - _min) / TABLE_PARTITION_SIZE;
            }

        public:

            ArrayTable() {
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

                // create partitions
                uint64_t numaDistributionCounter = 0;
                uint64_t offset = _min;
                while (offset <= _max) {
                    _partitions.emplace_back(
                        new(numaDistributionCounter++%basis::NUMA_NODE_COUNT) ArrayTablePartition(offset));
                    offset += TABLE_PARTITION_SIZE;
                }

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
                        uint64_t key = partitionColumnPartitionKey->getEntry(batchRowId);
                        // atomic increment the counter for the key
                        _partitions[getPartition(key)]->addInputPair(key);
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
                    ArrayTable<TABLE_PARTITION_SIZE>* _hashTable;

                public:
                    ATInputBuildTask(Batch<TABLE_PARTITION_SIZE>* batch, ArrayTable<TABLE_PARTITION_SIZE>* hashTable)
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
                // partition, i.e. write each tuple's key and row id into the corresponding 'ArrayTable'
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
                for(uint32_t partitionId=0; partitionId<_partitions.size(); partitionId++){
                    _hashTableHitCount += _partitions[partitionId]->getHashTableHitCount();
                    #ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
                    std::cout
                        << "partitionId: "      << partitionId
                        << ", HitCount: "       << _partitions[partitionId]->getHashTableHitCount()
                        << std::endl;
                    #endif
                }
                #ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
                    std::cout << "_hashTableHitCount: " << _hashTableHitCount << std::endl;
                #endif
                if(tupleCount != _hashTableHitCount){
                    // throw std::runtime_error("Detected inconsistency while building hash table: " + std::to_string(tupleCount) + " != " +
                    //     std::to_string(_hashTableHitCount) + " + " + std::to_string(_hashTableOverflowCount));
                }
                // set state
                _state=CHECKED;
            }

            bool isInPartitionRange(uint64_t key) {
                return key >= _min && key <= _max;
            }

            uint64_t probe(uint64_t key) {
                // if(this->_state != CHECKED){
                //     throw std::runtime_error("Array Hash Table not yet build");
                // }
                return _partitions[getPartition(key)]->probe(key);
            }

            void swap(ArrayTable<TABLE_PARTITION_SIZE>& other){
                // TODO potential deadlock
                std::unique_lock<std::mutex> uLock(_stateMutex);
                std::unique_lock<std::mutex> uLockOther(other._stateMutex);
                std::swap(_partitions, other._partitions);
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
