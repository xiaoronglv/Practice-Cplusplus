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

#ifndef TABLE_PARTITION_H
#define TABLE_PARTITION_H

#include "ColumnPartition.h"

#include <shared_mutex>
#include <unordered_map>


namespace database{

    template<uint32_t TABLE_PARTITION_SIZE>
    class Table;

    template<uint32_t TABLE_PARTITION_SIZE>
    class TablePartition : public basis::NumaAllocated {

        private:
            const Table<TABLE_PARTITION_SIZE>* _table;
            uint32_t _maxTablePartitionCardinality;
            uint32_t _numaNode;
            std::vector<ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<ColumnPartition<TABLE_PARTITION_SIZE>*>> _columnPartitions;

            mutable std::shared_mutex _availableRowsMutex;
            uint32_t _nextAvailableRowId;

        public:
            TablePartition(Table<TABLE_PARTITION_SIZE>* table, uint32_t numaNode) :
                    _table(table),
                    _maxTablePartitionCardinality(TABLE_PARTITION_SIZE),
                    _numaNode(numaNode),
                    _columnPartitions(basis::NumaAllocator<ColumnPartition<TABLE_PARTITION_SIZE>*>(numaNode)),
                    _nextAvailableRowId(0)
            {
                // create a column partition for each column in the table
                for(auto column : table->getColumns()){
                    _columnPartitions.push_back(column->createColumnPartition(numaNode));
                }
            }

            TablePartition(Table<TABLE_PARTITION_SIZE>* table, uint32_t numaNode, uint64_t cardinality, uint64_t partitionCardinality, char* addr)
            :       _table(table),
                    _maxTablePartitionCardinality(partitionCardinality),
                    _numaNode(numaNode),
                    _columnPartitions(basis::NumaAllocator<ColumnPartition<TABLE_PARTITION_SIZE>*>(numaNode)),
                    _nextAvailableRowId(partitionCardinality)
            {
                // create a column partition for each column in the table by reinterpret_cast
                const auto& columns = table->getColumns();
                for (uint32_t col = 0; col < columns.size(); ++col) {
                    _columnPartitions.push_back(columns[col]->createColumnPartition(addr));
                    if (col + 1 < columns.size()) {
                        addr += cardinality * sizeof(uint64_t);
                    }
                }
            }

            const Table<TABLE_PARTITION_SIZE>* getTable() const {
                return _table;
            }

            uint32_t getMaxTablePartitionCardinality() const {
                return _maxTablePartitionCardinality;
            }

            uint32_t getNumaNode() const {
                return _numaNode;
            }

            ColumnPartition<TABLE_PARTITION_SIZE>* getColumnPartition(uint32_t columnId) const {
                return _columnPartitions[columnId];
            }

            bool isFull() const {
                std::shared_lock<std::shared_mutex> sLock(_availableRowsMutex);
                return _nextAvailableRowId >= _maxTablePartitionCardinality;
            }

            uint32_t reserveEmptyRow(){
                // synchronized try to pop an empty partitionRowId from _availableRows
                std::unique_lock<std::shared_mutex> uLock(_availableRowsMutex);
                // if(_nextAvailableRowId >= _maxTablePartitionCardinality){
                //     throw std::runtime_error("Partition already full");
                // }
                uint32_t partitionRowId = _nextAvailableRowId;
                _nextAvailableRowId++;
                uLock.unlock();

                return partitionRowId;
            }

            bool isRowVisible(const uint32_t partitionRowId) const {
                return partitionRowId < _nextAvailableRowId;
            }
    };

    template<uint32_t TABLE_PARTITION_SIZE>
    struct PermanentTupleIdentifier{
        TablePartition<TABLE_PARTITION_SIZE>* _tablePartition; // raw pointer by design, since we create a lot of these identifiers
        uint32_t _partitionRowId;

        PermanentTupleIdentifier()
        : _tablePartition(nullptr), _partitionRowId(std::numeric_limits<uint32_t>::max()) {
        }

        PermanentTupleIdentifier(TablePartition<TABLE_PARTITION_SIZE>* tablePartition, const uint32_t partitionRowId)
        : _tablePartition(tablePartition), _partitionRowId(partitionRowId){
        }

        bool operator < (const PermanentTupleIdentifier<TABLE_PARTITION_SIZE>& other) const {
            if(_tablePartition == other._tablePartition){
                return _partitionRowId < other._partitionRowId;
            }
            return _tablePartition < other._tablePartition;
        }

        bool operator == (const PermanentTupleIdentifier<TABLE_PARTITION_SIZE>& other) const {
            return (_tablePartition==other._tablePartition) && (_partitionRowId==other._partitionRowId);
        }

        bool operator != (const PermanentTupleIdentifier<TABLE_PARTITION_SIZE>& other) const {
            return (_tablePartition!=other._tablePartition) || (_partitionRowId!=other._partitionRowId);
        }
    };

}

#endif
