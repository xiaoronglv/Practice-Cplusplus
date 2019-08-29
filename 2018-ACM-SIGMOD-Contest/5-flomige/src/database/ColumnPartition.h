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

#ifndef COLUMN_PARTITION_H
#define COLUMN_PARTITION_H

#include "../basis/TaskScheduler.h"
#include "../basis/Utilities.h"

#include <array>
#include <memory>
#include <forward_list>

// #include "emmintrin.h"

namespace database {

    template<uint32_t TABLE_PARTITION_SIZE>
    class ColumnPartition : public basis::NumaAllocated {

        // protected:

        public:
            std::array<uint64_t,TABLE_PARTITION_SIZE> _entries;

            ColumnPartition(){
            }

            virtual ~ColumnPartition(){
            };

            void setEntry(const uint32_t partitionRowId, uint64_t entry) {
                _entries[partitionRowId] = entry;
            }

            uint64_t getEntry(const uint32_t partitionRowId) const {
                return _entries[partitionRowId];
            }

            const std::array<uint64_t,TABLE_PARTITION_SIZE>& getEntries() const {
                return _entries;
            }
    };


    // global lock free pool of column partitions, column paritions get frequently allocated and deallocated during query processing
    // so it makes sense to pool them, the pool is partioned to avoid synchronization bottlecks, there is one global pool for each type
    template<uint32_t TABLE_PARTITION_SIZE>
    class ColumnPartitionPool {
        private:
            static std::vector<std::mutex> _poolPartitionMutexes;
            static std::atomic<uint32_t> _poolPartitionAccessCounter;
            static std::vector<std::forward_list<ColumnPartition<TABLE_PARTITION_SIZE>*>> _partitions; // _partitions[poolPartitionId][columnPartitionId]

        public:
            // try to push a column partition base to the global column partition of 'ValueType'
            static bool tryPushColumnPartition(ColumnPartition<TABLE_PARTITION_SIZE>* partition){
                // deallocate the partition, i.e., do nothing, when the memory is full
                if(partition != nullptr){
                    // determine and increment the pool partition id we will push into, it is ok when it overflows
                    uint32_t poolPartitionId = (_poolPartitionAccessCounter.fetch_add(1) % _partitions.size());
                    // lock the pool partition
                    std::unique_lock<std::mutex> uLock(_poolPartitionMutexes[poolPartitionId]);
                    // push the partition to this worker's partitions stack
                    _partitions[poolPartitionId].push_front(partition);
                    return true;
                }
                return false;
            }

            // try to get a column partition of 'ValueType' from this global pool
            static ColumnPartition<TABLE_PARTITION_SIZE>* tryPopColumnPartition(){
                // determine pool partition id and increment counter, it is ok when it overflows
                uint32_t poolPartitionId = (_poolPartitionAccessCounter.fetch_add(1) % _partitions.size());
                // lock the pool partition
                std::unique_lock<std::mutex> uLock(_poolPartitionMutexes[poolPartitionId]);
                // if there is a partition left in this pool
                if(!_partitions[poolPartitionId].empty()){
                    // pop a partition from this worker's partitions stack
                    ColumnPartition<TABLE_PARTITION_SIZE>* partition = _partitions[poolPartitionId].front();
                    _partitions[poolPartitionId].pop_front();
                    return partition;
                }
                return nullptr;
            }
    };

}

#endif
