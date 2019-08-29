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

#ifndef CONCISE_HASH_TABLE_ONE_PAYLOAD_H
#define CONCISE_HASH_TABLE_ONE_PAYLOAD_H

#include "Batch.h"
#include "PrefixBitmap.h"
#include "../basis/TaskScheduler.h"

#include <algorithm> // std::max
#include <climits> // CHAR_BIT
#include <cmath>
#include <unordered_map>

// #define PRINT_PARTITION_HITS_AND_OVERFLOWS
#ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
    #include <iostream>
#endif

namespace query_processor {


    //
    struct CHTEntryOnePayload{
        uint64_t _key;
        uint64_t _value;

        CHTEntryOnePayload()
        // we define a cht entry to be empty when the key is set to its maximum value,
        // in this case it is very important that this value is never used elsewhere, especially not as null value representation
        : _key(std::numeric_limits<uint64_t>::max()), _value(0) {
        }

        CHTEntryOnePayload(uint64_t key, uint64_t value)
        : _key(key), _value(value) {
        }

        bool isEmpty() const {
            // we define a cht entry to be empty when the key is set to its maximum value,
            // in this case it is very important that this value is never used elsewhere, especially not as null value representation
            return _key == std::numeric_limits<uint64_t>::max();
        }
    };

    template<uint32_t TABLE_PARTITION_SIZE>
    class ConciseHashTableOnePayload{

        public:

            struct OverflowResult{
                typename std::unordered_multimap<uint64_t, uint64_t>::const_iterator _itStart;
                typename std::unordered_multimap<uint64_t, uint64_t>::const_iterator _itEnd;
                uint64_t _size;
                uint32_t _sourceBatchRowId;
                OverflowResult(
                    typename std::unordered_multimap<uint64_t, uint64_t>::const_iterator& itStart,
                    typename std::unordered_multimap<uint64_t, uint64_t>::const_iterator& itEnd,
                    uint32_t sourceBatchRowId
                )
                : _itStart(itStart), _itEnd(itEnd), _size(std::distance(_itStart,_itEnd)), _sourceBatchRowId(sourceBatchRowId) {
                }

                OverflowResult() {
                }
            };

            class ConciseHashTableOnePayloadPartition : public basis::NumaAllocated {

                private:
                    std::unordered_multimap<uint64_t, uint64_t> _overflowTable;

                    uint32_t _partitionId;
                    uint32_t _numaNode;

                    std::vector<std::vector<CHTEntryOnePayload>> _inputs; // _inputs[workerId]
                    PrefixBitmap _bitmap;
                    std::vector<CHTEntryOnePayload, basis::NumaAllocator<CHTEntryOnePayload>> _array;

                    std::mutex _isBuildMutex;
                    bool _isBuild = false;

                public:
                    ConciseHashTableOnePayloadPartition(uint64_t partitionId, uint32_t numaNode, uint32_t workerCount)
                    : _partitionId(partitionId),
                        _numaNode(numaNode),
                        _inputs(workerCount),
                        _bitmap(numaNode),
                        _array(basis::NumaAllocator<CHTEntryOnePayload>(numaNode))
                    {
                    }

                    virtual ~ConciseHashTableOnePayloadPartition(){
                    }

                    void addInputPair(uint64_t key, uint64_t value, uint32_t workerId){
                        _inputs[workerId].emplace_back(key, value);
                    }

                    uint64_t getTupleCount(){
                        uint64_t tupleCount = 0;
                        for(const auto& workerInput : _inputs){
                            tupleCount += workerInput.size();
                        }
                        return tupleCount;
                    }

                    void setBitmapRowCount(){
                        // the final bitmap size has to be a multiple of '32 ' so that we can split the input into bitmap rows
                        uint64_t bitmapSize = std::ceil(static_cast<double>(CHT_BITMAP_MULTIPLICATOR) * getTupleCount() / 32.0) * 32; // HT_PARAMETER ' x * getTupleCount()'
                        uint64_t bitmapRowCount = std::max<uint64_t>(bitmapSize/32, 1);
                        // set the number of rows to the bitmap
                        _bitmap.setRowCount(bitmapRowCount);
                    }

                    uint64_t getHashTableHitCount() const {
                        // if(!_isBuild){
                        //     throw std::runtime_error("Requested partition build statistic ( _bitmap.getFillSize()) without building hash table");
                        // }
                        return _bitmap.getFillSize();
                    }

                    uint64_t getHashTableSize() const {
                        // if(!_isBuild){
                        //     throw std::runtime_error("Requested partition build statistic (_bitmap.getSize()) without building hash table");
                        // }
                        return _bitmap.getSize();
                    }

                    const PrefixBitmap& getBitmap() const{
                        return this->_bitmap;
                    }

                    const std::vector<CHTEntryOnePayload, basis::NumaAllocator<CHTEntryOnePayload>>& getArray() const{
                        return this->_array;
                    }

                    const std::unordered_multimap<uint64_t, uint64_t>& getOverflowTable(){
                        return this->_overflowTable;
                    }

                    void build(){
                        // ensures that the bitmap partition is build only once, lock the mutex and set '_isBuild' at the end of this function
                        std::unique_lock<std::mutex> uLock(this->_isBuildMutex);
                        // if(this->_isBuild){
                        //     throw std::runtime_error("Invoked hash map partition build again");
                        // }
                        // there is bitmap size and bitmap fill size!!!
                        uint64_t bitmapSize=this->_bitmap.getSize();
                        // scan over input and fill prefix bitmap
                        uint64_t hashValue;
                        for(const std::vector<CHTEntryOnePayload>& workerInputs : this->_inputs){
                            for(const CHTEntryOnePayload& pair : workerInputs){
                                // fill bitmap
                                hashValue = std::hash<uint64_t>{}(pair._key);
                                // try actual bucket
                                if(!this->_bitmap.tryToSetBit(hashValue%bitmapSize)) {
                                    // will be stored in overflow hash table
                                }
                            }
                        }
                        // calculate the prefixes, now that all bits are set
                        this->_bitmap.calculatePrefixes();
                        // determine fill size of bitmap and resize array, there is bitmap size and bitmap fill size!!!
                        this->_array.resize(this->_bitmap.getFillSize());
                        // scan again over input and fill array
                        for(const std::vector<CHTEntryOnePayload>& workerInputs : this->_inputs){
                            for(const CHTEntryOnePayload& pair : workerInputs){
                                hashValue = std::hash<uint64_t>{}(pair._key);
                                // calculate the index in the array from the bitmap
                                uint32_t index = this->_bitmap.calculateIndexBuild(hashValue%bitmapSize);
                                // try actual bucket
                                if(this->_array[index].isEmpty()){
                                    this->_array[index] = pair;
                                }
                                else {
                                    // insert into the multi map
                                    _overflowTable.emplace(pair._key, pair._value);
                                }
                            }
                        }
                        // set partition as build
                        this->_isBuild=true;
                    }

                    bool probe(
                            uint64_t key,
                            uint64_t hashValue,
                            uint64_t& mainResult,
                            std::vector<OverflowResult>& overflowResults,
                            uint32_t sourceBatchRowId
                    ) const {
                        // if(!this->_isBuild){
                        //     throw std::runtime_error("Concise Hash Table Partition not yet build");
                        // }
                        // there is bitmap size and bitmap fill size!!!
                        uint64_t bitmapSize = this->_bitmap.getSize();
                        uint32_t index;
                        // check actual bucket's bit, return false if bit not set
                        if(!this->_bitmap.calculateIndexProbe(hashValue % bitmapSize, index)){
                            return false;
                        }
                        // check actual bucket's value
                        if(key == this->_array[index]._key){
                            mainResult = this->_array[index]._value;
                            // if actual bit are set (i.e. we not yet returned) then we also have to check overflow table
                            auto range = _overflowTable.equal_range(key);
                            // if there are tuples in the range, we create an entry in the overflow results
                            if(range.first != range.second){
                                overflowResults.emplace_back(range.first, range.second, sourceBatchRowId);
                            }
                            return true;
                        } else {
                            // if actual bit is set and not the key, then we have to check the overflow table
                            auto range = _overflowTable.equal_range(key);
                            // if there are tuples in the range, we have to fill first the main result
                            if (range.first != range.second) {
                                mainResult = range.first->second;
                                ++range.first;
                                // if there are tuples in the range left, we create an entry in the overflow results
                                if(range.first != range.second){
                                    overflowResults.emplace_back(range.first, range.second, sourceBatchRowId);
                                }
                                return true;
                            } else {
                                // no key found
                                return false;
                            }
                        }
                    }

                    bool probeLastJoin(
                            uint64_t key,
                            uint64_t hashValue,
                            uint64_t& mainResult,
                            OverflowResult& overflowResults,
                            uint32_t sourceBatchRowId
                    ) const {
                        // if(!this->_isBuild){
                        //     throw std::runtime_error("Concise Hash Table Partition not yet build");
                        // }
                        // there is bitmap size and bitmap fill size!!!
                        uint64_t bitmapSize = this->_bitmap.getSize();
                        uint32_t index;
                        // check actual bucket's bit, return false if bit not set
                        if(!this->_bitmap.calculateIndexProbe(hashValue % bitmapSize, index)){
                            return false;
                        }
                        // check actual bucket's value
                        if(key == this->_array[index]._key){
                            mainResult = this->_array[index]._value;
                            // if actual bit are set (i.e. we not yet returned) then we also have to check overflow table
                            auto range = _overflowTable.equal_range(key);
                            // if there are tuples in the range, we create an entry in the overflow results
                            if (range.first != range.second){
                                overflowResults._itStart = range.first;
                                overflowResults._itEnd = range.second;
                                overflowResults._size = std::distance(range.first, range.second);
                                overflowResults._sourceBatchRowId = sourceBatchRowId;
                            } else {
                                // to indicate that there is no overflow result
                                overflowResults._size = 0;
                            }
                            return true;
                        } else {
                            // if actual bit is set and not the key, then we have to check the overflow table
                            auto range = _overflowTable.equal_range(key);
                            // if there are tuples in the range, we have to fill first the main result
                            if (range.first != range.second) {
                                mainResult = range.first->second;
                                ++range.first;
                                // if there are tuples in the range left, we create an entry in the overflow results
                                if(range.first != range.second){
                                    overflowResults._itStart = range.first;
                                    overflowResults._itEnd = range.second;
                                    overflowResults._size = std::distance(range.first, range.second);
                                    overflowResults._sourceBatchRowId = sourceBatchRowId;
                                } else {
                                    // to indicate that there is no overflow result
                                    overflowResults._size = 0;
                                }
                                return true;
                            } else {
                                // no key found
                                return false;
                            }
                        }
                    }

                    uint64_t getHashTableOverflowCount() const {
                        // if(!this->_isBuild){
                        //     throw std::runtime_error("Requested partition build statistic (_hashTableOverflowCount) without building hash table");
                        // }
                        return _overflowTable.size();
                    }
            };


        protected:
            std::vector<std::shared_ptr<ConciseHashTableOnePayloadPartition>> _partitions;
            uint32_t _partitionsCount = 1 << CHT_PARTITION_COUNT_BITS; // = 2 ^ CHT_PARTITION_COUNT_BITS, mind that '^' is the XOR operator ;)

            uint64_t _hashTableHitCount = 0;
            uint64_t _hashTableOverflowCount = 0;

            uint32_t _buildColumnPipelineIdKey;
            uint32_t _buildColumnPipelineIdValue;

            std::vector<uint32_t> _buildColumnPipelineIds;
            std::vector<uint32_t> _partitionNodes;  // partitionNodes[partitionId]

            HTState _state = UNINIZIALIZED;
            std::mutex _stateMutex;

            //
            // HT_PARAMETER
            uint32_t getPartition(uint64_t hashValue) const {
                // causes imbalanced partitions
                // TODo remove at some point
                // // take the most significant bits to determine the partition id
                // // 64 bit hashValue and 0 < CHT_PARTITION_COUNT_BITS <=32
                // uint32_t ret = hashValue >> (64-CHT_PARTITION_COUNT_BITS);
                // return ret;

                // // 'CHT_PARTITION_COUNT_BITS' is required to be larger than 32
                // // the partition id starts at the 17th lsb of the hash value, and takes at most the 32 following bits
                // // ffff ffff ffff ffff
                // //             ^^        for 'CHT_PARTITION_COUNT_BITS' = 8 we consider those 8 bits
                // // 0000 0000 0000 00ff
                // hashValue = hashValue << (64-16-CHT_PARTITION_COUNT_BITS);
                // hashValue = hashValue >> (64-16-CHT_PARTITION_COUNT_BITS+16);
                // return hashValue;

                // alternative 3: take the least significant bits
                hashValue = hashValue << (64-CHT_PARTITION_COUNT_BITS);
                hashValue = hashValue >> (64-CHT_PARTITION_COUNT_BITS);
                return hashValue;
            }


        public:
            ConciseHashTableOnePayload() {
            }

            void initializePartitions(uint32_t buildColumnPipelineIdKey, uint32_t buildColumnPipelineIdValue){ //estimatedTupleCount
                // ensures that the partitions are initialized only once, lock the mutex and change state at the end of this function
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != UNINIZIALIZED){
                //     throw std::runtime_error("Hash table initialization state error");
                // }
                // set members
                _buildColumnPipelineIdKey = buildColumnPipelineIdKey;
                _buildColumnPipelineIdValue = buildColumnPipelineIdValue;

                // if(CHT_PARTITION_COUNT_BITS>32){
                //     throw std::runtime_error("Overflow in partitions count");
                // }

                // decide which partition is allocated on which numa node, just round robin
                for(uint32_t i=0; i<_partitionsCount; i++){
                    _partitionNodes.emplace_back(i % basis::NUMA_NODE_COUNT);
                }

                // create partitions
                for(uint32_t partitionId=0; partitionId<_partitionsCount; partitionId++){
                    _partitions.emplace_back(
                        new(_partitionNodes[partitionId]) ConciseHashTableOnePayloadPartition( partitionId, _partitionNodes[partitionId],  basis::TaskScheduler::getWorkerCount()));
                }
                // set state
                _state=PARTITIONS_INI;
            }

            void pushBatch(Batch<TABLE_PARTITION_SIZE>* batch, uint32_t workerId){
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
                        // get the key, the hash value and the value to store
                        uint64_t key = partitionColumnPartitionKey->getEntry(batchRowId);
                        uint64_t hashValue = std::hash<uint64_t>{}(key);
                        uint32_t partitionId = getPartition(hashValue);
                        uint64_t value = partitionColumnPartitionValue->getEntry(batchRowId);
                        // add the key and tuple id to the partition
                        _partitions[partitionId]->addInputPair(key, value, workerId);
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

            class CHTInputPartitionTask : public basis::TaskBase {
                private:
                    Batch<TABLE_PARTITION_SIZE>* _batch;
                    ConciseHashTableOnePayload* _hashTable;

                public:
                    CHTInputPartitionTask(Batch<TABLE_PARTITION_SIZE>* batch, ConciseHashTableOnePayload* hashTable)
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
                // partition, i.e. write each tuple's key and row id into the corresponding 'ConciseHashTableOnePayloadPartition'
                basis::TaskGroup partitionTaskGroup;
                // for each batch, create a partition task
                for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& batch : buildSideBatches){
                    partitionTaskGroup.addTask(std::shared_ptr<basis::TaskBase>( new(batch->getNumaNode()) CHTInputPartitionTask(batch.get(),this) ));
                }
                partitionTaskGroup.execute();
                // set state
                _state=PARTITIONED;
            }

            class CHTPartitionBuildTask : public basis::TaskBase {
                private:
                    std::shared_ptr<ConciseHashTableOnePayloadPartition> _partition;

                public:
                    CHTPartitionBuildTask(uint32_t numaNode, std::shared_ptr<ConciseHashTableOnePayloadPartition> partition)
                    : TaskBase(basis::OLAP_TASK_PRIO, numaNode), _partition(partition){
                    }

                    void execute(){
                        _partition->setBitmapRowCount();
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
                        new(_partitionNodes[partitionId]) CHTPartitionBuildTask(_partitionNodes[partitionId], _partitions[partitionId])
                    ));
                }
                buildTaskGroup.execute();
                // set state
                _state=BUILD;
            }


            ConciseHashTableOnePayloadPartition* getPartitionPointer(uint64_t hashValue){
                uint32_t partitionId = this->getPartition(hashValue);
                return this->_partitions[partitionId].get();
            }

            bool probe(
                uint64_t key,
                uint64_t& mainResult,
                std::vector<OverflowResult>& overflowResults,
                uint32_t sourceBatchRowId
            ) const {
                // if(this->_state != CHECKED){
                //     throw std::runtime_error("Concise Hash Table not yet build");
                // }
                // determine key's partition id and forward the call to the corresponding partition
                uint64_t hashValue = std::hash<uint64_t>{}(key);
                uint32_t partitionId = getPartition(hashValue);
                return this->_partitions[partitionId].get()->probe(key, hashValue, mainResult, overflowResults, sourceBatchRowId);
            }

            bool probeLastJoin(
                uint64_t key,
                uint64_t& mainResult,
                OverflowResult& overflowResults,
                uint32_t sourceBatchRowId
            ) const {
                // if(this->_state != CHECKED){
                //     throw std::runtime_error("Concise Hash Table not yet build");
                // }
                // determine key's partition id and forward the call to the corresponding partition
                uint64_t hashValue = std::hash<uint64_t>{}(key);
                uint32_t partitionId = getPartition(hashValue);
                return this->_partitions[partitionId].get()->probeLastJoin(key, hashValue, mainResult, overflowResults, sourceBatchRowId);
            }

            void check(uint64_t tupleCount){
                // ensures that the bitmap is build
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != BUILD){
                //     throw std::runtime_error("Hash table check state error");
                // }
                // run over the partitions and calculate stats
                // for(std::shared_ptr<ConciseHashTableOnePayloadPartition> partition : _partitions){
                for(uint32_t partitionId=0; partitionId<_partitions.size(); partitionId++){
                    _hashTableHitCount += _partitions[partitionId]->getHashTableHitCount();
                    _hashTableOverflowCount += _partitions[partitionId]->getHashTableOverflowCount();
                    #ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
                    std::cout
                        << "partitionId: "      << partitionId
                        << ", HT Size: "        << _partitions[partitionId]->getHashTableSize()
                        << ", HitCount: "       << _partitions[partitionId]->getHashTableHitCount()
                        << ", OverflowCount: "  << _partitions[partitionId]->getHashTableOverflowCount()
                        << std::endl;
                    #endif
                }
                #ifdef PRINT_PARTITION_HITS_AND_OVERFLOWS
                    std::cout << "_hashTableHitCount: " << _hashTableHitCount << ", _hashTableOverflowCount: " << _hashTableOverflowCount << std::endl;
                #endif
                if(tupleCount != _hashTableHitCount + _hashTableOverflowCount){
                    // throw std::runtime_error("Detected inconsistency while building hash table: " + std::to_string(tupleCount) + " != " +
                    //     std::to_string(_hashTableHitCount) + " + " + std::to_string(_hashTableOverflowCount));
                }
                // set state
                _state=CHECKED;
            }

            void swap(ConciseHashTableOnePayload& other){
                // TODO potential deadlock
                std::unique_lock<std::mutex> uLock(_stateMutex);
                std::unique_lock<std::mutex> uLockOther(other._stateMutex);
                _partitions.swap(other._partitions);
                std::swap(_partitionsCount, other._partitionsCount);
                std::swap(_hashTableHitCount, other._hashTableHitCount);
                std::swap(_hashTableOverflowCount, other._hashTableOverflowCount);
                std::swap(_buildColumnPipelineIdKey, other._buildColumnPipelineIdKey);
                std::swap(_buildColumnPipelineIdValue, other._buildColumnPipelineIdValue);
                _partitionNodes.swap(other._partitionNodes);
                std::swap(_state, other._state);
            }

            uint64_t getHashTableHitCount() const {
                // if(_state != CHECKED){
                //     throw std::runtime_error("Hash table stat error 2");
                // }
                return _hashTableHitCount;
            }

            uint64_t getHashTableOverflowCount() const {
                // if(_state != CHECKED){
                //     throw std::runtime_error("Hash table stat error 3");
                // }
                return _hashTableOverflowCount;
            }

    };

}

#endif
