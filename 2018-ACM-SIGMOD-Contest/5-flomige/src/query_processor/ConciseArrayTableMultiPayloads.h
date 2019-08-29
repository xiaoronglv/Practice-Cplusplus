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

#ifndef CONCISE_ARRAY_TABLE_MULTI_PAYLOADS_H
#define CONCISE_ARRAY_TABLE_MULTI_PAYLOADS_H

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
    template<uint32_t TABLE_PARTITION_SIZE>
    struct CATPartitionEntry{
            uint64_t _index;
            BatchTupleIdentifier<TABLE_PARTITION_SIZE> _value;

            CATPartitionEntry(uint64_t index, Batch<TABLE_PARTITION_SIZE>* batch, const uint32_t batchRowId)
            : _index(index), _value(batch, batchRowId) {
            }
    };


    //
    // CATEntry = BatchTupleIdentifier<TABLE_PARTITION_SIZE> _value; (no need to stroe the key, since the key is the index)


    template<uint32_t TABLE_PARTITION_SIZE>
    class ConciseArrayTableMultiPayloads{

        public:

            struct MainResult {
                std::vector<BatchTupleIdentifier<TABLE_PARTITION_SIZE>, basis::NumaAllocator<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>>* _array = nullptr;
                uint32_t _start = 0;
                uint32_t _end = 0;
            };

            struct OverflowResult {
                std::vector<BatchTupleIdentifier<TABLE_PARTITION_SIZE>, basis::NumaAllocator<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>>* _array;
                uint32_t _start;
                uint32_t _end;
                uint32_t _size;
                uint32_t _sourceBatchRowId;
                OverflowResult(
                    std::vector<BatchTupleIdentifier<TABLE_PARTITION_SIZE>, basis::NumaAllocator<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>>* array,
                    uint32_t start,
                    uint32_t end,
                    uint32_t sourceBatchRowId
                ) : _array(array), _start(start), _end(end), _size(_end - _start), _sourceBatchRowId(sourceBatchRowId) {
                }
            };

            class ConciseArrayTableMultiPayloadsPartition : public basis::NumaAllocated {

                protected:
                    uint32_t _partitionId;
                    uint32_t _numaNode;

                    uint64_t _offset;
                    uint64_t _nextOffset; // exclusive

                    std::vector<std::vector<CATPartitionEntry<TABLE_PARTITION_SIZE>>> _inputs; // _inputs[workerId]
                    PrefixArray<uint32_t> _prefixArray;
                    std::vector<BatchTupleIdentifier<TABLE_PARTITION_SIZE>, basis::NumaAllocator<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>> _array;

                    std::mutex _isBuildMutex;
                    bool _isBuild = false;

                public:
                    ConciseArrayTableMultiPayloadsPartition(uint64_t partitionId, uint32_t numaNode, uint64_t offset, uint64_t nextOffset, uint32_t workerCount)
                    :   _partitionId(partitionId),
                        _numaNode(numaNode),
                        _offset(offset),
                        _nextOffset(nextOffset),
                        _inputs(workerCount),
                        _prefixArray(numaNode, nextOffset - offset),
                        _array(basis::NumaAllocator<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>(numaNode)) {
                    }

                    virtual ~ConciseArrayTableMultiPayloadsPartition(){
                    }

                    void addInputPair(uint64_t key, Batch<TABLE_PARTITION_SIZE>* batch, const uint32_t batchRowId, uint32_t workerId){
                        // check if key is valid
                        // if (key < _offset || key >= _nextOffset) {
                        //     throw std::runtime_error("key is not in the range between the offset and the upper bound 1");
                        // }
                        _inputs[workerId].emplace_back(key - _offset,batch,batchRowId); // store already the index
                    }

                    uint64_t getTupleCount(){
                        uint64_t tupleCount = 0;
                        for(const auto& workerInput : _inputs){
                            tupleCount += workerInput.size();
                        }
                        return tupleCount;
                    }

                    void build() {
                        // ensures that the bitmap partition is build only once, lock the mutex and set '_isBuild' at the end of this function
                        std::unique_lock<std::mutex> uLock(_isBuildMutex);
                        // if(_isBuild){
                        //     throw std::runtime_error("Invoked hash map partition build again");
                        // }
                        // scan over input and fill prefix array
                        for(const std::vector<CATPartitionEntry<TABLE_PARTITION_SIZE>>& workerInputs : _inputs){
                            for(const CATPartitionEntry<TABLE_PARTITION_SIZE>& pair : workerInputs){
                                // fill prefix array
                                _prefixArray.addEntry(pair._index);
                            }
                        }
                        // calculate the prefixes, now that all counts are set
                        _prefixArray.calculatePrefixes();
                        // determine fill size of prefix array and resize array
                        _array.resize(_prefixArray.getFillSize());
                        // scan again over input and fill array
                        for(const std::vector<CATPartitionEntry<TABLE_PARTITION_SIZE>>& workerInputs : _inputs){
                            for(const CATPartitionEntry<TABLE_PARTITION_SIZE>& pair : workerInputs){
                                _array[_prefixArray.calculateIndexBuild(pair._index)] = pair._value;
                            }
                        }
                        // check if the prefix was build right
                        _prefixArray.checkIndexBuild();
                        // set partition as build
                        _isBuild=true;
                    }

                    bool probe(uint64_t key, MainResult& mainResult) {
                        // if(!_isBuild){
                        //     throw std::runtime_error("Concise Hash Table Partition not yet build");
                        // }
                        // store the pointer to the array
                        mainResult._array = &_array;
                        // return the range for the results from the prefix array
                        return _prefixArray.calculateIndexProbe(key - _offset, mainResult._start, mainResult._end);
                    }

                    uint64_t getHashTableHitCount() const {
                        // if(!_isBuild){
                        //     throw std::runtime_error("Requested partition build statistic ( _prefixArray.getFillSize()) without building hash table");
                        // }
                        return _prefixArray.getFillSize();
                    }
            };


        private:
            std::vector<std::shared_ptr<ConciseArrayTableMultiPayloadsPartition>> _partitions;

            uint64_t _hashTableHitCount = 0;

            std::vector<uint32_t> _buildColumnPipelineIds;
            std::vector<uint32_t> _partitionNodes;  // partitionNodes[partitionId]

            uint64_t _min;
            uint64_t _max;
            uint64_t _partitionOffset;

            HTState _state = UNINIZIALIZED;
            std::mutex _stateMutex;

            // returns the partition
            uint32_t getPartition(uint64_t key) const {
                return (key - _min) / _partitionOffset;
            }


        public:
            ConciseArrayTableMultiPayloads(){
            }

            void initializePartitions(std::vector<uint32_t> buildColumnPipelineIds, uint64_t min, uint64_t max){ //estimatedTupleCount
                // ensures that the partitions are initialized only once, lock the mutex and change state at the end of this function
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != UNINIZIALIZED){
                //     throw std::runtime_error("Hash table initialization state error");
                // }
                // set members, we took a copy with the function parameter
                _buildColumnPipelineIds.swap(buildColumnPipelineIds);
                _min = min;
                _max = max;
                _partitionOffset = basis::Utilities::uint64_ceil(_max - _min + 1, CAT_PARTITION_COUNT);

                // decide which partition is allocated on which numa node, just round robin
                for(uint32_t i=0; i<CAT_PARTITION_COUNT; i++){
                    _partitionNodes.emplace_back(i % basis::NUMA_NODE_COUNT);
                }

                // create partitions
                uint64_t offset = _min;
                for(uint32_t partitionId=0; partitionId<CAT_PARTITION_COUNT; partitionId++){
                    _partitions.emplace_back(
                        new(_partitionNodes[partitionId]) ConciseArrayTableMultiPayloadsPartition( partitionId, _partitionNodes[partitionId], offset, offset + _partitionOffset,
                             basis::TaskScheduler::getWorkerCount()));
                    offset += _partitionOffset;
                }
                // set state
                _state=PARTITIONS_INI;
            }


            void pushBatch(Batch<TABLE_PARTITION_SIZE>* batch, uint32_t workerId){
                // ensures that the bitmap is initialized
                // if(_state != PARTITIONS_INI){
                //     throw std::runtime_error("Hash table push state error");
                // }
                // run over each row in the batch
                for(uint32_t batchRowId=0; batchRowId < batch->getCurrentSize(); batchRowId++){
                    // check if row is still valid
                    if(batch->isRowValid(batchRowId)){
                        // cast the column partition to get the key
                        const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartition = batch->getColumnPartition(_buildColumnPipelineIds[0]);
                        uint64_t key = partitionColumnPartition->getEntry(batchRowId);
                        _partitions[getPartition(key)]->addInputPair(key, batch, batchRowId, workerId);
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
                    ConciseArrayTableMultiPayloads<TABLE_PARTITION_SIZE>* _hashTable;

                public:
                    CATInputPartitionTask(Batch<TABLE_PARTITION_SIZE>* batch, ConciseArrayTableMultiPayloads<TABLE_PARTITION_SIZE>* hashTable)
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
                // partition, i.e. write each tuple's key and row id into the corresponding 'ConciseArrayTableMultiPayloads'
                basis::TaskGroup partitionTaskGroup;
                // for each batch, create a partion task
                for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& batch : buildSideBatches){
                    partitionTaskGroup.addTask(std::shared_ptr<basis::TaskBase>( new(batch->getNumaNode()) CATInputPartitionTask(batch.get(),this) ));
                }
                partitionTaskGroup.execute();
                // set state
                _state=PARTITIONED;
            }

            class CATPartitionBuildTask : public basis::TaskBase {
                private:
                    std::shared_ptr<ConciseArrayTableMultiPayloadsPartition> _partition;

                public:
                    CATPartitionBuildTask(uint32_t numaNode, std::shared_ptr<ConciseArrayTableMultiPayloadsPartition> partition)
                    : TaskBase(basis::OLAP_TASK_PRIO, numaNode), _partition(partition){
                    }

                    void execute(){
                        _partition->build();
                    }
            };

            void build(){
                // ensures that the bitmap is partitioned
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != PARTITIONED){
                //     throw std::runtime_error("Hash table build state error");
                // }
                // build the partitions, create a task for each partition
                basis::TaskGroup buildTaskGroup;
                for(uint32_t partitionId=0; partitionId<_partitions.size(); partitionId++){
                    buildTaskGroup.addTask(std::shared_ptr<basis::TaskBase>(
                        new(_partitionNodes[partitionId]) CATPartitionBuildTask(_partitionNodes[partitionId], _partitions[partitionId])
                    ));
                }
                buildTaskGroup.execute();
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

            bool probe(uint64_t key, MainResult& mainResult) {
                // if(this->_state != CHECKED){
                //     throw std::runtime_error("Array Hash Table not yet build");
                // }
                return _partitions[getPartition(key)]->probe(key, mainResult);
            }

            void swap(ConciseArrayTableMultiPayloads<TABLE_PARTITION_SIZE>& other){
                // TODO potential deadlock
                std::unique_lock<std::mutex> uLock(_stateMutex);
                std::unique_lock<std::mutex> uLockOther(other._stateMutex);
                _partitions.swap(other._partitions);
                std::swap(_hashTableHitCount, other._hashTableHitCount);
                _buildColumnPipelineIds.swap(other._buildColumnPipelineIds);
                _partitionNodes.swap(other._partitionNodes);
                std::swap(_min, other._min);
                std::swap(_max, other._max);
                std::swap(_partitionOffset, other._partitionOffset);
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
