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

#ifndef CONCISE_HASH_TABLE_MULTI_PAYLOADS_H
#define CONCISE_HASH_TABLE_MULTI_PAYLOADS_H

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

    // hash function for std tuples maps
    template<uint64_t HashSeed, class... KeyTypes>
    class TupleHashFunction {
        private:
            // hash function, HT_PARAMETER
            template<class KeyType>
            static void hashFunction(uint64_t& hashValue, KeyType key){
                // // alternative 1:
                // hashValue = key * (key + hashValue); // multiplicative hash function for value: xi * (xi + hi)

                // alternative 2:
                // hashValue ^= (                                          // xor
                //         (key * (key + 982451653))                       // multiplicative hash function: xi * (xi + hi)
                //     +   (0x9e3779b9 + (hashValue<<6) + (hashValue>>2))  // add a shifted seed
                // );

                // alternative 3:
                hashValue ^= (                                          // xor
                        std::hash<KeyType>{}(key)                       // standard hash
                    +   0x9e3779b9 + (hashValue<<6) + (hashValue>>2)    // add a shifted seed
                );
            }

            // recursion base
            template<int TupleIndex, class CurrentType>
            static void evaluateTuple(uint64_t& hashValue, const std::tuple<KeyTypes...>& key){
                // apply hash function on current tuple entry
                hashFunction<CurrentType>(hashValue, std::get<TupleIndex>(key));
            }

            // recursive tuple evaluation
            template<int TupleIndex, class CurrentType, class NextType, class... OtherTypes>
            static void evaluateTuple(uint64_t& hashValue, const std::tuple<KeyTypes...>& key){
                // apply hash function on current tuple entry
                hashFunction<CurrentType>(hashValue, std::get<TupleIndex>(key));
                // recursive invocation
                evaluateTuple<TupleIndex+1, NextType, OtherTypes...>(hashValue, key);
            }

        public:
            //
            static uint64_t calculate(const std::tuple<KeyTypes...>& key){
                uint64_t hashValue = HashSeed;
                evaluateTuple<0, KeyTypes...>(hashValue, key);
                return hashValue;
            }
    };


    // hash function for cht
    template <class... KeyTypes>
    class CHTHashFunction {
        public:
            static uint64_t calculate(const std::tuple<KeyTypes...>& tuple) {
                // HT_PARAMETER
                // return TupleHashFunction<18446744073709551557ul, KeyTypes...>::calculate(tuple);
                return TupleHashFunction<86028157, KeyTypes...>::calculate(tuple);
            }
    };


    // hash function for overflow hash table which is an stl container
    template <class Key>
    struct OverflowHash;

    template <class... KeyTypes>
    struct OverflowHash<std::tuple<KeyTypes...>> {
        uint64_t operator()(const std::tuple<KeyTypes...>& tuple) const {
            // HT_PARAMETER
            // return TupleHashFunction<12073803838594344817ul, KeyTypes...>::calculate(tuple);
            return TupleHashFunction<104395301, KeyTypes...>::calculate(tuple);
        }
    };


    //
    template <uint32_t TABLE_PARTITION_SIZE, class FirstKeyType, class... OtherKeyTypes>
    class CHTEntryMultiPayloads{
        private:
            std::tuple<FirstKeyType, OtherKeyTypes...> _key;
            BatchTupleIdentifier<TABLE_PARTITION_SIZE> _value;

        public:
            CHTEntryMultiPayloads()
            : _value(nullptr,0){
                // we define a cht entry to be empty when the first value in the key tuple is set to its maximum value,
                // in this case it is very important that this value is never used elsewhere, especially not as null value represantation
                std::get<0>(_key) = std::numeric_limits<FirstKeyType>::max();
            }

            CHTEntryMultiPayloads(std::tuple<FirstKeyType, OtherKeyTypes...> key, Batch<TABLE_PARTITION_SIZE>* batch, const uint32_t batchRowId)
            : _key(key), _value(batch, batchRowId) {
            }

            const std::tuple<FirstKeyType, OtherKeyTypes...>& getKey() const {
                return _key;
            }

            const BatchTupleIdentifier<TABLE_PARTITION_SIZE>& getValue() const {
                return _value;
            }

            bool isEmpty() const {
                // we define an cht entry to be empty when the first value in the key tuple is set to its maximum value,
                // in this case it is very important that this value is never used elsewhere, especially not as null value represantation
                return std::get<0>(_key) == std::numeric_limits<FirstKeyType>::max();
            }
    };



    //
    template<uint32_t TABLE_PARTITION_SIZE, class... KeyTypes>
    class TupleCreator{
        private:
            // recursion base
            template<uint32_t CurrentIndex, class CurrentType>
            static bool createTupleCheckNull(
                Batch<TABLE_PARTITION_SIZE>* batch,
                uint32_t batchRowId,
                const std::vector<uint32_t>& buildColumnPipelineIds,
                std::tuple<KeyTypes...>& targetTuple
            ){
                // cast the column partition
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartition = batch->getColumnPartition(buildColumnPipelineIds[CurrentIndex]);
                // set the value in the tuple
                std::get<CurrentIndex>(targetTuple)=partitionColumnPartition->getEntry(batchRowId);
                return true;
            }

            // recursive function
            template<uint32_t CurrentIndex, class CurrentType, class NextType, class... OtherTypes>
            static bool createTupleCheckNull(
                Batch<TABLE_PARTITION_SIZE>* batch,
                uint32_t batchRowId,
                const std::vector<uint32_t>& buildColumnPipelineIds,
                std::tuple<KeyTypes...>& targetTuple
            ){
                // get the column partition
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* partitionColumnPartition = batch->getColumnPartition(buildColumnPipelineIds[CurrentIndex]);
                // set the value in the tuple
                std::get<CurrentIndex>(targetTuple)=partitionColumnPartition->getEntry(batchRowId);
                // recursive invocation on the next column id
                return createTupleCheckNull<CurrentIndex+1,NextType,OtherTypes...>(batch, batchRowId, buildColumnPipelineIds, targetTuple);
            }

        public:
                static bool createCheckNull(
                Batch<TABLE_PARTITION_SIZE>* batch,
                uint32_t batchRowId,
                const std::vector<uint32_t>& buildColumnPipelineIds,
                std::tuple<KeyTypes...>& targetTuple
            ){
                return createTupleCheckNull<0,KeyTypes...>(batch, batchRowId, buildColumnPipelineIds, targetTuple);
            }

    };

    template<uint32_t TABLE_PARTITION_SIZE, class... KeyTypes>
    class ConciseHashTableMultiPayloads{

        public:

            struct OverflowResult{
                const typename std::unordered_multimap<std::tuple<KeyTypes...>, BatchTupleIdentifier<TABLE_PARTITION_SIZE>, OverflowHash<std::tuple<KeyTypes...>>>::const_iterator _itStart;
                const typename std::unordered_multimap<std::tuple<KeyTypes...>, BatchTupleIdentifier<TABLE_PARTITION_SIZE>, OverflowHash<std::tuple<KeyTypes...>>>::const_iterator _itEnd;
                uint64_t _size;
                uint32_t _sourceBatchRowId;
                OverflowResult(
                    typename std::unordered_multimap<std::tuple<KeyTypes...>, BatchTupleIdentifier<TABLE_PARTITION_SIZE>, OverflowHash<std::tuple<KeyTypes...>>>::const_iterator& itStart,
                    typename std::unordered_multimap<std::tuple<KeyTypes...>, BatchTupleIdentifier<TABLE_PARTITION_SIZE>, OverflowHash<std::tuple<KeyTypes...>>>::const_iterator& itEnd,
                    uint32_t sourceBatchRowId
                )
                : _itStart(itStart), _itEnd(itEnd), _size(std::distance(_itStart,_itEnd)), _sourceBatchRowId(sourceBatchRowId) {
                }
            };

            class ConciseHashTableMultiPayloadsPartition : public basis::NumaAllocated {

                private:
                    std::unordered_multimap<std::tuple<KeyTypes...>, BatchTupleIdentifier<TABLE_PARTITION_SIZE>, OverflowHash<std::tuple<KeyTypes...>>> _overflowTable;

                protected:
                    uint32_t _partitionId;
                    uint32_t _numaNode;
                    std::vector<std::vector<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>>> _inputs; // _inputs[workerId]
                    PrefixBitmap _bitmap;
                    std::vector<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>, basis::NumaAllocator<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>>> _array;

                    std::mutex _isBuildMutex;
                    bool _isBuild = false;

                public:
                    ConciseHashTableMultiPayloadsPartition(uint64_t partitionId, uint32_t numaNode, uint32_t workerCount)
                    : _partitionId(partitionId),
                        _numaNode(numaNode),
                        _inputs(workerCount),
                        _bitmap(numaNode),
                        _array(basis::NumaAllocator<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>>(numaNode))
                    {
                    }

                    virtual ~ConciseHashTableMultiPayloadsPartition(){
                    }

                    void addInputPair(std::tuple<KeyTypes...> key, Batch<TABLE_PARTITION_SIZE>* batch, const uint32_t batchRowId, uint32_t workerId){
                        _inputs[workerId].emplace_back(key,batch,batchRowId);
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

                    const std::vector<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>, basis::NumaAllocator<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>>>& getArray() const{
                        return this->_array;
                    }

                    const std::unordered_multimap<std::tuple<KeyTypes...>, BatchTupleIdentifier<TABLE_PARTITION_SIZE>, OverflowHash<std::tuple<KeyTypes...>>>& getOverflowTable(){
                        return this->_overflowTable;
                    }

                    void build(){
                        // identical to 'ConciseHashTableMultiPayloadsPartition::build()'
                        // ensures that the bitmap partition is build only once, lock the mutex and set '_isBuild' at the end of this function
                        std::unique_lock<std::mutex> uLock(this->_isBuildMutex);
                        // if(this->_isBuild){
                        //     throw std::runtime_error("Invoked hash map partition build again");
                        // }
                        // there is bitmap size and bitmap fill size!!!
                        uint64_t bitmapSize=this->_bitmap.getSize();
                        // scan over input and fill prefix bitmap
                        uint64_t hashValue;
                        for(const std::vector<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>>& workerInputs : this->_inputs){
                            for(const CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>& pair : workerInputs){
                                // fill bitmap
                                hashValue = CHTHashFunction<KeyTypes...>::calculate(pair.getKey());
                                // try actual bucket
                                if(!this->_bitmap.tryToSetBit(hashValue%bitmapSize)){
                                    // try next bucket
                                    if(!this->_bitmap.tryToSetBit((hashValue+1)%bitmapSize)){
                                        // will be stored in overflow hash table
                                    }
                                }
                            }
                        }
                        // calculate the prefixes, now that all bits are set
                        this->_bitmap.calculatePrefixes();
                        // determine fill size of bitmap and resize array, there is bitmap size and bitmap fill size!!!
                        this->_array.resize(this->_bitmap.getFillSize());
                        // scan again over input and fill array
                        for(const std::vector<CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>>& workerInputs : this->_inputs){
                            for(const CHTEntryMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>& pair : workerInputs){
                                hashValue = CHTHashFunction<KeyTypes...>::calculate(pair.getKey());
                                // calculate the index in the array from the bitmap
                                uint32_t index = this->_bitmap.calculateIndexBuild(hashValue%bitmapSize);
                                // try actual bucket
                                if(this->_array[index].isEmpty()){
                                    this->_array[index] = pair;
                                }
                                else{
                                    // try next bucket
                                    index++;
                                    index %= this->_array.size();
                                    if(this->_array[index].isEmpty()){
                                        this->_array[index] = pair;
                                    }
                                    // write to overflow hash table
                                    else{
                                        // insert into the multi map
                                        _overflowTable.emplace(pair.getKey(), pair.getValue());
                                    }
                                }
                            }
                        }
                        // set partition as build
                        this->_isBuild=true;
                    }

                    void probe(
                            const std::tuple<KeyTypes...>& key,
                            uint64_t hashValue,
                            std::vector<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>& mainResult,
                            std::vector<OverflowResult>& overflowResults,
                            uint32_t sourceBatchRowId
                    ) const {
                        // if(!this->_isBuild){
                        //     throw std::runtime_error("Concise Hash Table Partition not yet build");
                        // }
                        // clear the main result
                        mainResult.clear();
                        // there is bitmap size and bitmap fill size!!!
                        uint64_t bitmapSize = this->_bitmap.getSize();
                        uint32_t index;
                        // check actual bucket's bit, return if bit not set
                        if(!this->_bitmap.calculateIndexProbe(hashValue % bitmapSize, index)){
                            return;
                        }
                        // check actual bucket's value
                        if(key == this->_array[index].getKey()){
                            mainResult.push_back(this->_array[index].getValue());
                        }
                        // check if next bucket's bit was set, return if bit not set so that we avoid checking the overflow table
                        if(!this->_bitmap.calculateIndexProbe((hashValue + 1) % bitmapSize, index)){
                            return;
                        }
                        // check next bucket's value
                        if(key == this->_array[index].getKey()){
                            mainResult.push_back(this->_array[index].getValue());
                        }
                        // if both bits (for actual and next bucket) are set (i.e. we not yet returned) then we also have to check overflow table
                        auto range = _overflowTable.equal_range(key);
                        // we have to ensure that we fill the main result first, its size is limited to '2' to avoid to much copying overhead,
                        // so in case the main result is not yet completely filled, we fill it with potential overflow results and move itOverflowResultStart forward
                        while(range.first != range.second && mainResult.size() < 2){
                            mainResult.push_back(range.first->second);
                            ++range.first;
                        }
                        // if there are tuples in the range left, we create an entry in the overflow results
                        if(range.first != range.second){
                            overflowResults.emplace_back(range.first, range.second, sourceBatchRowId);
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
            std::vector<std::shared_ptr<ConciseHashTableMultiPayloadsPartition>> _partitions;
            uint32_t _partitionsCount = 1 << CHT_PARTITION_COUNT_BITS; // = 2 ^ CHT_PARTITION_COUNT_BITS, mind that '^' is the XOR operator ;)

            uint64_t _hashTableHitCount = 0;
            uint64_t _hashTableOverflowCount = 0;

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

                // alternative 3: take the least significatn bits
                hashValue = hashValue << (64-CHT_PARTITION_COUNT_BITS);
                hashValue = hashValue >> (64-CHT_PARTITION_COUNT_BITS);
                return hashValue;
            }


        public:
            ConciseHashTableMultiPayloads() {
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
                        // create a tuple with the entries from the batch column partitions corresponding to '_buildColumnPipelineIds',
                        // also checks for null values since we do not join on them, so we insert only when there are no null values in the key
                        // TODO make a second version (ideally templated) for 'not null' columns that avoids this potential branch missprediction
                        std::tuple<KeyTypes...> key;
                        if(TupleCreator<TABLE_PARTITION_SIZE, KeyTypes...>::createCheckNull(batch, batchRowId, _buildColumnPipelineIds, key)){
                            // calucualte the hash value and determine the partition id
                            uint64_t hashValue = CHTHashFunction<KeyTypes...>::calculate(key);
                            uint32_t partitionId = getPartition(hashValue);

                            // add the key and tuple id to the partition
                            // TODO maybe also pass the hash value to avoid calculating it again two times when building the concise hash table
                            _partitions[partitionId]->addInputPair(key, batch, batchRowId, workerId);
                        }
                    }
                }
            }

            void initializePartitions(std::vector<uint32_t> buildColumnPipelineIds){ //estimatedTupleCount
                // ensures that the partitions are initialized only once, lock the mutex and change state at the end of this function
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != UNINIZIALIZED){
                //     throw std::runtime_error("Hash table initialization state error");
                // }
                // set members, we took a copy with the function parameter
                _buildColumnPipelineIds.swap(buildColumnPipelineIds);

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
                        new(_partitionNodes[partitionId]) ConciseHashTableMultiPayloadsPartition( partitionId, _partitionNodes[partitionId],  basis::TaskScheduler::getWorkerCount()));
                }
                // set state
                _state=PARTITIONS_INI;
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
                    ConciseHashTableMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>* _hashTable;

                public:
                    CHTInputPartitionTask(Batch<TABLE_PARTITION_SIZE>* batch, ConciseHashTableMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>* hashTable)
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
                // partition, i.e. write each tuple's key and row id into the corresponding 'ConciseHashTableMultiPayloadsPartition'
                basis::TaskGroup partitionTaskGroup;
                // TODO const batch!?
                // for each batch, create a partion task
                for(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>& batch : buildSideBatches){
                    partitionTaskGroup.addTask(std::shared_ptr<basis::TaskBase>( new(batch->getNumaNode()) CHTInputPartitionTask(batch.get(),this) ));
                }
                partitionTaskGroup.execute();
                // set state
                _state=PARTITIONED;
            }

            class CHTPartitionBuildTask : public basis::TaskBase {
                private:
                    std::shared_ptr<ConciseHashTableMultiPayloadsPartition> _partition;

                public:
                    CHTPartitionBuildTask(uint32_t numaNode, std::shared_ptr<ConciseHashTableMultiPayloadsPartition> partition)
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


            ConciseHashTableMultiPayloadsPartition* getPartitionPointer(uint64_t hashValue){
                uint32_t partitionId = this->getPartition(hashValue);
                return this->_partitions[partitionId].get();
            }

            void probe(
                const std::tuple<KeyTypes...>& key,
                std::vector<BatchTupleIdentifier<TABLE_PARTITION_SIZE>>& mainResult,
                std::vector<OverflowResult>& overflowResults,
                uint32_t sourceBatchRowId
            ) const {
                // if(this->_state != CHECKED){
                //     throw std::runtime_error("Concise Hash Table not yet build");
                // }
                // determine key's partition id and forward the call to the corresponding partition
                // see CHTInputPartitionTask::execute() for further information
                uint64_t hashValue = CHTHashFunction<KeyTypes...>::calculate(key);
                uint32_t partitionId = this->getPartition(hashValue);
                // using a std::static_pointer_cast here is very expensive because this is invoked for each tuple that is probed
                this->_partitions[partitionId].get()->probe(key, hashValue, mainResult, overflowResults, sourceBatchRowId);
            }

            void check(uint64_t tupleCount){
                // ensures that the bitmap is build
                std::unique_lock<std::mutex> uLock(_stateMutex);
                // if(_state != BUILD){
                //     throw std::runtime_error("Hash table check state error");
                // }
                // run over the partitions and calculate stats
                // for(std::shared_ptr<ConciseHashTableMultiPayloadsPartition> partition : _partitions){
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

            void swap(ConciseHashTableMultiPayloads<TABLE_PARTITION_SIZE, KeyTypes...>& other){
                // TODO potential deadlock
                std::unique_lock<std::mutex> uLock(_stateMutex);
                std::unique_lock<std::mutex> uLockOther(other._stateMutex);
                _partitions.swap(other._partitions);
                std::swap(_partitionsCount, other._partitionsCount);
                std::swap(_hashTableHitCount, other._hashTableHitCount);
                std::swap(_hashTableOverflowCount, other._hashTableOverflowCount);
                _buildColumnPipelineIds.swap(other._buildColumnPipelineIds);
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
