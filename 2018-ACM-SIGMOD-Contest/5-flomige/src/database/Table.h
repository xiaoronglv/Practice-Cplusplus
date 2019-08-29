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

#ifndef TABLE_H
#define TABLE_H

#include "PermanentColumn.h"

#include <atomic>
#include <map>
#include <stack>

namespace database {

    // basically contains the meta data to create tables with columns, these object are passed to the database to create the actual tables,
    // it enables consistency by e.g. avoiding that columns are added to a table when its already filled with data
    template<uint32_t TABLE_PARTITION_SIZE>
    class TableConstructionDescription {
        public:
            // base class for column construction descriptions mainly containing a pure virtual function to create the actual column object
            class ColumnConstructionDescription {
                protected:
                    // members for the column name and to indicate if the column is part of the primary key, has a unique contraint, will get an index,
                    // has a not null constraint, or a foreign key constraint (pointer to foreign key column)
                    const std::string _name;

                public:
                    // constructor with parameters to set the members
                    ColumnConstructionDescription(const std::string& name) : _name(name) {
                    };

                    // function to create the actual column object out of column description
                    std::shared_ptr<PermanentColumn<TABLE_PARTITION_SIZE>> createColumn(uint32_t columnId, const Table<TABLE_PARTITION_SIZE>* table, char* addr) {
                        return std::make_shared<PermanentColumn<TABLE_PARTITION_SIZE>>(columnId, _name, table, addr);
                    }
            };

        private:
            // members for the table name and a container for keeps the column descritions
            std::string _name;
            std::vector<std::shared_ptr<ColumnConstructionDescription>> _columnConstructionDescriptions;

        public:
            // constructor that takes the table name a parameter
            TableConstructionDescription(std::string name)
            : _name(name) {
            }

            // creates and adds an encoded column construction description
            // template<class ValueType, typename std::enable_if< std::is_same<ValueType,uint64_t>::value || std::is_same<ValueType,long>::value, int >::type = 0>
            void addColumn(std::string name){
                _columnConstructionDescriptions.emplace_back(new ColumnConstructionDescription(name));
            }

            // returns the name of the table that should be created, this is invoked by the database when it creates a new table object to add
            const std::string& getName() const {
                return _name;
            }

            // returns a reference to the container with the column descriptions, when the database creates the table, it runs over this container
            // and invokes 'createColumn' on each entry to create and add the specified column object
            const std::vector<std::shared_ptr<TableConstructionDescription::ColumnConstructionDescription>>& getColumnConstructionDescriptions() const {
                return _columnConstructionDescriptions;
            }
    };


    // the main (and only) table class of the database, containing the columns and the table partitions, offers an interface to insert tuples,
    // retrieve columns and table partitions (data/tuples)
    template<uint32_t TABLE_PARTITION_SIZE>
    class Table {
        private:
            // members for table name
            std::string _name;

            // the columns in this table, all containers store the same columns but potentially in a different order
            std::vector<std::shared_ptr<PermanentColumn<TABLE_PARTITION_SIZE>>> _columns;
            std::vector<std::shared_ptr<const PermanentColumn<TABLE_PARTITION_SIZE>>> _columnsConst;
            std::unordered_map<std::string, std::shared_ptr<PermanentColumn<TABLE_PARTITION_SIZE>>> _columnsMap;

            // the table partitions of this table stored in different data structures, protected by a mutex, and associated with different meta information
            mutable std::shared_mutex _tablePartitionsMutex;
            std::vector<std::shared_ptr<TablePartition<TABLE_PARTITION_SIZE>>> _tablePartitions;
            std::vector<std::shared_ptr<TablePartition<TABLE_PARTITION_SIZE>>> _nonFullPartitions;
            std::map<uint32_t,uint64_t> _tablePartitionDistribution;

            // table statistics, the version independent cardinality of this table
            std::atomic<uint64_t> _latestCardinality;

            // to distribute the table partitions over the numa nodes
            uint32_t _numaPartitionCounter = 0;

            // returns the numa node on which a new table partition should be allocated on, basically implements equal distribution stragety over the numa nodes
            uint32_t getNumaNodeToPlaceTablePartition(){
                // numa partioning strategy
                return _numaPartitionCounter++ % basis::NUMA_NODE_COUNT;
            }

            // returns the tuple id to an empty row, once we insert a new tuple (we insert also on updates) we invoke this function to get an empty row in a table partition
            void reserveEmptyRow(PermanentTupleIdentifier<TABLE_PARTITION_SIZE>& rowId){
                bool successful=false;
                rowId._tablePartition = nullptr;
                rowId._partitionRowId = 0;
                std::unique_lock<std::shared_mutex> uLock(_tablePartitionsMutex,std::defer_lock);
                while(!successful){
                    // try to fetch a non full partition
                    uLock.lock();
                    while( rowId._tablePartition==nullptr && !_nonFullPartitions.empty() ){
                        // rowId._tablePartition=_nonFullPartitions.top().get();
                        // try to distrubute the tuples over all non empty partitions
                        uint32_t tablePartitionIndex = _latestCardinality%_nonFullPartitions.size();
                        rowId._tablePartition = _nonFullPartitions[tablePartitionIndex].get();
                        if(rowId._tablePartition->isFull()){
                            rowId._tablePartition = nullptr;
                            // remove the partition from '_nonFullPartitions' when it is already full
                            _nonFullPartitions.erase(_nonFullPartitions.begin() + tablePartitionIndex);
                        }
                    }
                    // if there is no partition to insert, create a new partition
                    if(rowId._tablePartition==nullptr){
                        // numa partioning strategy
                        uint32_t numaNode = getNumaNodeToPlaceTablePartition();
                        // create a new numa allocated table partition
                        std::shared_ptr<TablePartition<TABLE_PARTITION_SIZE>> tablePartition( new(numaNode) TablePartition<TABLE_PARTITION_SIZE>(this, numaNode) );
                        rowId._tablePartition = tablePartition.get();
                        _tablePartitions.push_back(tablePartition);
                        _nonFullPartitions.push_back(tablePartition);
                    }
                    uLock.unlock();
                    // synchronized, get a row id from the partition, it is possible that the partition becomes full inbetween isFull() and reserveEmptyRow()
                    try{
                        rowId._partitionRowId = rowId._tablePartition->reserveEmptyRow();
                        successful=true;
                    }
                    catch(std::exception& e){
                        rowId._tablePartition = nullptr;
                        rowId._partitionRowId = 0;
                    }
                }
            }


        public:
            // constructor taking the a description object containing the specified table and column parameters,
            // the cardinality and the address from the mmap file (column binary store)
            Table(TableConstructionDescription<TABLE_PARTITION_SIZE>& descr, uint64_t cardinality, char* addr)
            : _name(descr.getName()) {
                basis::Utilities::validName(_name);
                // create columns from description
                char* columnOffset = addr;
                for(auto columnDescr : descr.getColumnConstructionDescriptions()){
                    uint32_t columnId = _columns.size();
                    std::shared_ptr<PermanentColumn<TABLE_PARTITION_SIZE>> column = columnDescr->createColumn(columnId, this, columnOffset);

                    // add the column pointer to the corresponding table data structures
                    auto ret = _columnsMap.emplace(column->getName(), column);
                    if(!ret.second){
                        throw std::runtime_error("Column name is already given");
                    }
                    _columns.push_back(column);
                    _columnsConst.push_back(column);

                    // update the cardinality for the stats of the permanent column
                    column->setCardinality(cardinality);

                    // update column offset
                    columnOffset += cardinality * sizeof(uint64_t);
                }
                // initialize the atomic cardinality counter with the cardinality parameter
                std::atomic_init(&_latestCardinality, cardinality);

                // calculate the number of needed table partitions
                uint64_t numTablePartitions = basis::Utilities::uint64_ceil(cardinality, TABLE_PARTITION_SIZE);
                uint64_t lastTablePartitionSize;
                if ((cardinality % TABLE_PARTITION_SIZE) > 0) {
                    lastTablePartitionSize = cardinality % TABLE_PARTITION_SIZE;
                } else {
                    lastTablePartitionSize = TABLE_PARTITION_SIZE;
                }

                for (uint64_t partition = 0; partition < numTablePartitions; ++partition) {
                    // numa partioning strategy
                    uint32_t numaNode = getNumaNodeToPlaceTablePartition();
                    // create a new numa allocated 'full' table partition
                    char* tablePartitionOffset = addr;
                    uint64_t tablePartitionSize;
                    // update addr by the offset of the current table and get the partition size
                    if (partition + 1 < numTablePartitions) {
                        tablePartitionSize = TABLE_PARTITION_SIZE;
                        addr += TABLE_PARTITION_SIZE * sizeof(uint64_t);
                    } else {
                        tablePartitionSize = lastTablePartitionSize;
                        addr += lastTablePartitionSize * sizeof(uint64_t);
                    }
                    std::shared_ptr<TablePartition<TABLE_PARTITION_SIZE>> tablePartition( new(numaNode) TablePartition<TABLE_PARTITION_SIZE>(this, numaNode, cardinality, tablePartitionSize, tablePartitionOffset) );
                    _tablePartitions.push_back(tablePartition);
                    // no non full partitions
                }
            }


            // returns the name of the database
            const std::string& getName() const {
                return _name;
            }


            // returns the columns in the database in the right order
            const std::vector<std::shared_ptr<const database::PermanentColumn<TABLE_PARTITION_SIZE>>>& getColumns() const {
                return _columnsConst;
            }


            // returns all permanent columns this table is made of
            void getColumns(std::vector<PermanentColumn<TABLE_PARTITION_SIZE>*>& permanentColumns) {
                // run over permanent columns
                for(std::shared_ptr<PermanentColumn<TABLE_PARTITION_SIZE>>& column : _columns){
                    // push permanent column into the target
                    permanentColumns.push_back(static_cast<PermanentColumn<TABLE_PARTITION_SIZE>*>(column.get()));
                }
            }


            // returns a const (read only) pointer to the column in the table having the specified name, used for queries
            std::shared_ptr<const PermanentColumn<TABLE_PARTITION_SIZE>> retrieveColumnByNameReadOnly(std::string name) const {
                // get upper case representation
                basis::Utilities::validName(name);
                // search for column in _columns map and return the pointer if found
                auto res = _columnsMap.find(name);
                if(res == _columnsMap.end()){
                    throw std::runtime_error("Column not found");
                }
                return res->second;
            }


            // returns a regular (read write) pointer to the column in the table having the specified name, used for write transactions
            std::shared_ptr<PermanentColumn<TABLE_PARTITION_SIZE>> retrieveColumnByNameReadWrite(std::string name) {
                // get upper case representation
                basis::Utilities::validName(name);
                // search for column in _columns map and return the pointer if found
                auto res = _columnsMap.find(name);
                if(res == _columnsMap.end()){
                    throw std::runtime_error("Column not found");
                }
                return res->second;
            }

            // returns all the table partitions this table is made of
            void getTablePartitions(std::vector<TablePartition<TABLE_PARTITION_SIZE>*>& tablePartitions) const {
                // get a shared lock on this table's table-partitions
                std::shared_lock<std::shared_mutex> sLock(_tablePartitionsMutex);
                // run over table partitions
                for(std::shared_ptr<TablePartition<TABLE_PARTITION_SIZE>> tp : _tablePartitions){
                    // push table partitions into the target
                    tablePartitions.push_back(static_cast<TablePartition<TABLE_PARTITION_SIZE>*>(tp.get()));
                }
            }

            // returns the cardinality of the table i.e. the number of rows, since the cardinality estimation for the base tables is not consitency
            // relevant in query optimization, we avoid providing the cardinality of a certain version and provide only the latest version's cardinality
            uint64_t getLatestCardinality() const {
                return _latestCardinality;
            }

    };

}

#endif
