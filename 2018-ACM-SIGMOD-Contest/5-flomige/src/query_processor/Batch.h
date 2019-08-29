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

#ifndef BATCH_H
#define BATCH_H

#include "TemporaryColumn.h"

#include <bitset>
#include <algorithm>
#include <iostream>

// #define DEBUG

namespace query_processor {

    // different stats for a hash table
    enum HTState {UNINIZIALIZED, PARTITIONS_INI, PARTITIONED, BUILD, CHECKED};

    // sets the size of the bitmap of the cht, i.e., size of the bitmap = CHT_BITMAP_MULTIPLICATOR * getTupleCount()
    static uint32_t CHT_BITMAP_MULTIPLICATOR = 64;
    // sets the number of partitions, i.e., number of partitions  = 2 ^ CHT_PARTITION_COUNT_BITS
    static uint32_t CHT_PARTITION_COUNT_BITS = 5;

    // sets the number of partitions for the Concise Array Tables
    static uint32_t CAT_PARTITION_COUNT = 40;

    // used mainly in the 'PipelineColumnsContainer' to store pipeline column's names and column pointer
    // a vector of this is used as return of 'PipelineColumnsContainer::getColumns()'
    template<uint32_t TABLE_PARTITION_SIZE>
    struct NameColumnStruct {
        std::vector<std::string> _names;
        std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> _column;
        bool _isProjectionColumn;
        uint32_t _projectionCount;
        uint32_t _operatorCount;
        uint64_t _min;
        uint64_t _max;
        bool _isDistinct;
        NameColumnStruct(
            std::vector<std::string> names,
            std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> column,
            bool isProjectionColumn,
            uint32_t projectionCount,
            uint32_t operatorCount,
            uint64_t min,
            uint64_t max,
            bool isDistinct
        ) : _names(names), _column(column), _isProjectionColumn(isProjectionColumn), _projectionCount(projectionCount), _operatorCount(operatorCount),
            _min(min), _max(max), _isDistinct(isDistinct) {
            _max = std::max(_min, _max);
        }

        // check if name is stored
        bool isName(std::string& name) {
            return std::find(_names.begin(), _names.end(), name) != _names.end();
        }

        // add name
        void addName(std::string& name, bool isProjectionColumn, uint64_t otherEstimatedMin, uint64_t otherEstimatedMax, bool isDistinct) {
            // check if name is already there
            if (!isName(name)) {
                _names.push_back(name);
            }
            _isProjectionColumn |= isProjectionColumn;
            _min = std::max(_min, otherEstimatedMin);
            _max = std::min(_max, otherEstimatedMax);
            // make sure that the max value is no smaller than the min value
            _max = std::max(_min, _max);
            // update distinct count
            _isDistinct &= isDistinct;

        }

        // updates the statistic after a filter
        void addFilterStatistic(uint64_t otherEstimatedMin, uint64_t otherEstimatedMax) {
            _min = std::max(_min, otherEstimatedMin);
            _max = std::min(_max, otherEstimatedMax);
            // make sure that the max value is no smaller than the min value
            _max = std::max(_min, _max);
        }

        // increase the operator count
        void increaseOperatorCount(uint32_t increment) {
            // check if operator count is max, then we don't do anything
            if (_operatorCount == std::numeric_limits<uint32_t>::max()) {
                return;
            }
            _operatorCount += increment;
        }

        // increase the projection count
        void increaseProjectionCount(uint32_t increment) {
            // check if operator count is max, then we don't do anything
            if (_projectionCount == std::numeric_limits<uint32_t>::max()) {
                return;
            }
            _projectionCount += increment;
        }

        // decrease the operator count
        void decreaseOperatorCount(uint32_t decrement) {
            // check if operator count is max, then we don't do anything
            if (_operatorCount == std::numeric_limits<uint32_t>::max()) {
                return;
            }
            // if (decrement > _operatorCount) {
            //     throw std::runtime_error("It is not allowed to decrease the operator count to less than zero.");
            // }
            _operatorCount -= decrement;
        }

    };

    // used as return of the 'PipelineColumnsContainer::find()' to provide the find result the component that search for a column by name,
    // it contains the pipeline column id and the column pointer
    template<uint32_t TABLE_PARTITION_SIZE>
    struct ColumnIdColumnStruct {
        uint32_t _pipelineColumnId;
        std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> _column;
        ColumnIdColumnStruct(uint32_t pipelineColumnId, std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> column)
        : _pipelineColumnId(pipelineColumnId), _column(column){
        }
    };

    // hybrid of map and vector containing a vector of name column structs where the index of the vector respresents the pipeline column id,
    // in order to search for column names it also contains a map of name to pipeline column i.e. the index in the vector of name column structs,
    // this is class is also responsible for giving new pipeline column ids (as return of the 'addColumn' function)
    // furthermore it contains a mapping of column type to column ids i.e. which column ids are of a certain type (but the orther direction: type -> ids)
    template<uint32_t TABLE_PARTITION_SIZE>
    class PipelineColumnsContainer{
        private:
            std::map<std::string, uint32_t> _mapping; // TODO rename to 'mappings'
            std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>> _columns; // _columns[columnId]

            // mappings: type -> column ids
            // std::vector<uint32_t> _idsRawColumns;
            std::vector<uint32_t> _idsRawColumnsUInt64;

        public:
            // add a new column to this container, ensures that the name is unique, and returns the pipeline column id of the added column
            uint32_t addNewColumn(std::vector<std::string> pipelineColumnNames, std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> column,
                bool isProjectionColumn, uint32_t projectionCount, uint32_t operatorCount, uint64_t min, uint64_t max, bool isDistinct)
            {
                // valid name for column name
                // TODO or not necessary!?
                // determine a new column id
                uint32_t newColumnId = _columns.size();
                // try to insert the name pipeline column id mapping
                for (std::string pipelineColumnName : pipelineColumnNames) {
                    _mapping.emplace(pipelineColumnName,newColumnId);
                    // auto res = _mapping.emplace(pipelineColumnName,newColumnId);
                    // if(!res.second){
                    //     throw std::runtime_error("Pipeline column name or alias '" + pipelineColumnName + "' already exists");
                    // }
                }
                // add the column information i.e. pipeline column name and column pointer into the '_columns' vector
                _columns.emplace_back(pipelineColumnNames, column, isProjectionColumn, projectionCount, operatorCount, min, max, isDistinct);
                _idsRawColumnsUInt64.push_back(newColumnId);
                return newColumnId;
            }

            // add an additional column name to an existing column. used for join columns
            void addAdditionalColumnNames(std::string& pipelineColumnName, std::vector<std::string>& additionalColumnNames,
                bool isProjectionColumn, uint32_t projectionCount, uint32_t addToOperatorCount, uint64_t min, uint64_t max, bool isDistinct
            ) {
                // valid name for column name
                // TODO or not necessary!?
                // search for the column name
                auto res = _mapping.find(pipelineColumnName);
                // if(res == _mapping.end()) {
                //     // print the container
                //     print();
                //     // throw the exception
                //     throw std::runtime_error("Pipeline column name or alias '" + pipelineColumnName + "' not found");
                // }
                // add additional column names to the already existing name column struct
                for (uint32_t i = 0; i < additionalColumnNames.size(); ++i) {
                    _columns[res->second].addName(additionalColumnNames[i], isProjectionColumn, min, max, isDistinct);
                    // add the column information to the mapping
                    _mapping.emplace(additionalColumnNames[i], res->second);
                }
                // update operator count
                _columns[res->second].increaseProjectionCount(projectionCount);
                _columns[res->second].increaseOperatorCount(addToOperatorCount);
            }

            // update the min and max statistic after a filter
            void addFilterStatistic(std::string& pipelineColumnName, uint64_t otherEstimatedMin, uint64_t otherEstimatedMax) {
                // valid name for column name
                // TODO or not necessary!?
                // search for the column name
                auto res = _mapping.find(pipelineColumnName);
                // if(res == _mapping.end()) {
                //     // print the container
                //     print();
                //     // throw the exception
                //     throw std::runtime_error("Pipeline column name or alias '" + pipelineColumnName + "' not found");
                // }
                _columns[res->second].addFilterStatistic(otherEstimatedMin, otherEstimatedMax);
            }

            // decrease the operator count for a column
            void decreaseOperatorCount(std::string& pipelineColumnName, uint32_t decrement) {
                // valid name for column name
                // TODO or not necessary!?
                // search for the column name
                auto res = _mapping.find(pipelineColumnName);
                // if(res == _mapping.end()) {
                //     // print the container
                //     print();
                //     // throw the exception
                //     throw std::runtime_error("Pipeline column name or alias '" + pipelineColumnName + "' not found");
                // }
                // decrease the operator count for this column
                _columns[res->second].decreaseOperatorCount(decrement);
            }

            // to search for a column by their current pipeline column name
            ColumnIdColumnStruct<TABLE_PARTITION_SIZE> find(std::string& pipelineColumnName) const {
                // valid name for column name
                // TODO or not necessary!?
                auto res = _mapping.find(pipelineColumnName);
                // if(res == _mapping.end()) {
                //     // print the container
                //     print();
                //     // throw the exception
                //     throw std::runtime_error("Pipeline column name or alias '" + pipelineColumnName + "' not found");
                // }
                return ColumnIdColumnStruct(res->second, _columns[res->second]._column);
            }

            bool isColumnDistinct(std::string& pipelineColumnName) {
                // valid name for column name
                // TODO or not necessary!?
                // search for the column name
                auto res = _mapping.find(pipelineColumnName);
                // if(res == _mapping.end()) {
                //     // print the container
                //     print();
                //     // throw the exception
                //     throw std::runtime_error("Pipeline column name or alias '" + pipelineColumnName + "' not found");
                // }
                // return if the column is distinct or not
                return _columns[res->second]._isDistinct;
            }

            void setAllColumnsNonDistinct() {
                for (NameColumnStruct<TABLE_PARTITION_SIZE>& column : _columns) {
                    column._isDistinct = false;
                }
            }

            // provides the current columns vector where the index represents the pipeline column id
            std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& getColumns() {
                return _columns;
            }

            // necessary for consuming this container, e.g. a new pipeline is build on a breaker
            void swap(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& other){ // other
                _mapping.swap(other._mapping);
                _columns.swap(other._columns);
                _idsRawColumnsUInt64.swap(other._idsRawColumnsUInt64);
            }

            // necessary for consuming this container, e.g. a new pipeline is build on a breaker
            bool empty(){
                return _mapping.empty() && _columns.empty();
            }

            // returns the column ids of all columns of columns of a certain type
            const std::vector<uint32_t>& getIdsRawColumns() const{
                return _idsRawColumnsUInt64;
            }

            //prints this container's content
            void print() const {
                std::cout << "  _mappings:" << std::endl;
                for(const auto& entry : _mapping){
                    //
                    std::cout << "      " << entry.first << " " << entry.second << std::endl;
                }
                std::cout << "  _columns:" << std::endl;
                for(const auto& entry : _columns){
                    for (auto& name : entry._names)
                    std::cout << "      " << name << std::endl;
                }
                std::cout << "  _idsRawColumnsUInt64:" << std::endl;
                for(const auto& entry : _idsRawColumnsUInt64){
                    std::cout << "      " << entry << std::endl;
                }
            }
    };


    class ColumnIdMappingsContainer{
        private:
            std::map<uint32_t, uint32_t> _uInt64Mappings;
        public:
            void addRawColumnIdMapping(uint32_t buildBreakerColumnId, uint32_t probePipeColumnId);

            void swap(ColumnIdMappingsContainer& other);

            const std::map<uint32_t, uint32_t>& getMapping() const;
    };


    //
    template<uint32_t TABLE_PARTITION_SIZE>
    class Batch : public basis::NumaAllocated {
        protected:
            uint32_t _numaNode;
            uint32_t _maxTablePartitionCardinality;
            uint32_t _validRowsCount;
            uint32_t _nextRowId;
            std::bitset<TABLE_PARTITION_SIZE> _validRows;
            std::vector<database::ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>> _columnPartitions;
            std::vector<database::ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>> _temporaryColumnPartitions;
            bool _isChecksumsUsed;
            std::vector<uint64_t> _checksums;

        public:

            // constructor for batches that are created on base tables / base table partitions
            Batch(const database::TablePartition<TABLE_PARTITION_SIZE>* tablePartition, const std::vector<uint32_t>& initialColumnIds)
            :   _numaNode(tablePartition->getNumaNode()),
                _maxTablePartitionCardinality(tablePartition->getMaxTablePartitionCardinality()),
                _validRowsCount(0),
                _nextRowId(TABLE_PARTITION_SIZE),
                _columnPartitions(basis::NumaAllocator<std::shared_ptr<const database::ColumnPartition<TABLE_PARTITION_SIZE>>>(_numaNode)),
                _temporaryColumnPartitions(basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>(_numaNode)),
                _isChecksumsUsed(false),
                _checksums()
            {
                // add column partitions to this batch, run over columns
                for(uint32_t baseTableColumnId : initialColumnIds){
                    // fetch column partition and add it
                    _columnPartitions.push_back(tablePartition->getColumnPartition(baseTableColumnId));
                    // also extend '_temporaryColumnPartitions' since column partitions in both containers have to correspond to the pipeline column ids
                    _temporaryColumnPartitions.push_back(nullptr);
                    // also extend the checksums columns
                    _checksums.push_back(0);
                }
                // run over batch and validate visible rows
                for(uint32_t batchRowId=0; batchRowId<_maxTablePartitionCardinality; batchRowId++){
                    if (tablePartition->isRowVisible(batchRowId)) {
                        validateRow(batchRowId);
                    }
                }
            }


            // constructor for batches that allow inserting tuples, used in breaker starter, when repartitioning the input, or consolidating batches
            Batch(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& pipelineColumns, uint32_t numaNode)
            :   _numaNode(numaNode),
                _maxTablePartitionCardinality(TABLE_PARTITION_SIZE),
                _validRowsCount(0),
                _nextRowId(0),
                _columnPartitions(basis::NumaAllocator<std::shared_ptr<const database::ColumnPartition<TABLE_PARTITION_SIZE>>>(_numaNode)),
                _temporaryColumnPartitions(basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>(_numaNode)),
                _isChecksumsUsed(false),
                _checksums()
            {
                 // run over breaker columns and add corresponding column partitions to this batch
                for(auto& nameColumnStruct : pipelineColumns.getColumns() ){
                    // just add columns, batches created with this constructor are initially empty i.e. have no tuples ( _validRows.size() == 0 )
                    database::ColumnPartition<TABLE_PARTITION_SIZE>* columnPartition = nameColumnStruct._column->createColumnPartitionTryPool(_numaNode);
                    _columnPartitions.push_back(columnPartition);
                    _temporaryColumnPartitions.push_back(columnPartition);
                    _checksums.push_back(0);
                }
            }


            // destructor that returns all column partitions back into the global column partitions pools
            ~Batch(){
                // try to push the temporary column partitions to its corresponding column partition pools
                for(auto& tempPartition : _temporaryColumnPartitions){
                    database::ColumnPartitionPool<TABLE_PARTITION_SIZE>::tryPushColumnPartition(tempPartition);
                }
            }

            // to add column partitions during join operators
            void addNewColumnPartition(std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>> column) {
                database::ColumnPartition<TABLE_PARTITION_SIZE>* columnPartition = column->createColumnPartitionTryPool(_numaNode);
                _columnPartitions.push_back(columnPartition);
                _temporaryColumnPartitions.push_back(columnPartition);
                _checksums.push_back(0);
            }

            // as 'getColumnPartition' but returns raw pointer
            const database::ColumnPartition<TABLE_PARTITION_SIZE>* getColumnPartition(uint32_t pipelineColumnId) const {
                return _columnPartitions[pipelineColumnId];
            }

            // used when copying tuples e.g. in build breaker
            const std::vector<database::ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>>&
            getColumnPartitions() const {
                return _columnPartitions;
            }

            const std::vector<database::ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>>&
            getTemporaryColumnPartitions() const {
                return _temporaryColumnPartitions;
            }

            void insertIntoColumnPartition(uint32_t pipelineColumnId, uint32_t partitionRowId, uint64_t value){
                _temporaryColumnPartitions[pipelineColumnId]->setEntry(partitionRowId, value);
            }

            // invoked in projection operators to reorder the column partitions in the batch corresponding to the projection
            void reArrangeColumnPartitions(const std::vector<uint32_t>& oldColumnIds){ // oldColumnIds[newColumnId]
                // create new numa allocated column partition vectors and checksums
                // TODO passing them directly creates compile error
                basis::NumaAllocator<std::shared_ptr<const database::ColumnPartition<TABLE_PARTITION_SIZE>>> constColumnPartitionAlo(_numaNode);
                basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*> columnPartitionAlo(_numaNode);
                std::vector<database::ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>>
                    newColumnPartitions(constColumnPartitionAlo);
                std::vector<database::ColumnPartition<TABLE_PARTITION_SIZE>*, basis::NumaAllocator<database::ColumnPartition<TABLE_PARTITION_SIZE>*>>
                    newTemporaryColumnPartitions(columnPartitionAlo);
                std::vector<uint64_t> newChecksums;
                // reserve space in the new vectors
                newColumnPartitions.reserve(oldColumnIds.size());
                newTemporaryColumnPartitions.reserve(oldColumnIds.size());
                newChecksums.reserve(oldColumnIds.size());
                // run over the old column ids and push old column partitions into the new numa allocated column partition vector and checksums
                for(uint32_t newColumnId=0; newColumnId<oldColumnIds.size(); newColumnId++){
                    newColumnPartitions.push_back( _columnPartitions[ oldColumnIds.at(newColumnId) ] );
                    newTemporaryColumnPartitions.push_back( _temporaryColumnPartitions[ oldColumnIds.at(newColumnId) ] );
                    newChecksums.push_back( _checksums[oldColumnIds.at(newColumnId) ] );
                }
                // swap new and old column partitions and checksums
                _columnPartitions.swap(newColumnPartitions);
                _temporaryColumnPartitions.swap(newTemporaryColumnPartitions);
                _checksums.swap(newChecksums);
            }

            // invoked when we start with an empty batch and fill it with tuples e.g. when partioning the input in hash join build
            bool addTupleIfPossible(uint32_t& rowId){
                // there has to be space for at least one more tuple, otherwise return false
                if(_nextRowId >= TABLE_PARTITION_SIZE){
                    return false;
                }
                // get a row id
                rowId = _nextRowId;
                // validate the row
                _validRows[_nextRowId] = 1;
                // increase valid row count and nextRowId
                _validRowsCount++;
                _nextRowId++;
                return true;
            }

            void setIsChecksumUsed(bool isChecksumUsed) {
                _isChecksumsUsed = isChecksumUsed;
            }

            bool isChecksumUsed() {
                return _isChecksumsUsed;
            }

            std::vector<uint64_t>& getChecksums() {
                return _checksums;
            }

            bool isRowValid(const uint32_t batchRowId) const {
                return _validRows[batchRowId] == 1;
            }

            const std::bitset<TABLE_PARTITION_SIZE>& getValidRows() {
                return _validRows;
            }

            void validateRow(const uint32_t batchRowId){
                if(!_validRows[batchRowId]){
                    _validRows[batchRowId] = 1;
                    ++_validRowsCount;
                }
            }

            void invalidateRow(const uint32_t batchRowId){
                if(_validRows[batchRowId]){
                    _validRows[batchRowId] = 0;
                    --_validRowsCount;
                }
            }

            // returns the number of rows that are currently valid in the batch
            uint32_t getValidRowCount() const {
                return _validRowsCount;
            }

            // return the maximum size a batch can have, this corresponds to 'database::TABLE_PARTITION_SIZE'
            static uint32_t getMaximumSize() {
                return TABLE_PARTITION_SIZE;
            }

            // returns the current size of the batch, note that this can be different to 'getMaximumSize()' and 'getValidRowCount()'
            uint32_t getCurrentSize() const {
                // return _nextRowId;
                return _maxTablePartitionCardinality;
            }

            // returns the numa node this batch is allocated on
            uint32_t getNumaNode() const {
                return _numaNode;
            }

            // copies the specified tuple in this batch to another batch using the specified 'pipeColumns',
            // does not perform any checks if the target batch has the same structure or if the 'pipeColumns' fit
            void copyTupleTo(uint32_t sourceBatchRowId, PipelineColumnsContainer<TABLE_PARTITION_SIZE>& pipeColumns, Batch* targetBatch, uint32_t targetBatchRowId) const {
                for(uint32_t columnId = 0; columnId < pipeColumns.getColumns().size(); ++columnId){
                    // get and cast source column partition
                    const database::ColumnPartition<TABLE_PARTITION_SIZE>* sourceColumnPartition = _columnPartitions[columnId];
                    // copy the entry from the original tuple into the duplicate tuple
                    targetBatch->insertIntoColumnPartition(columnId, targetBatchRowId, sourceColumnPartition->getEntry(sourceBatchRowId));
                }
            }

            // copies corresponding to a mapping the entries from a tuple in this batch to another tuple in another batch
            // this used in joins, does not perform any checks if the target batch has the same structure or if the 'mapping' is correct
            void copyEntiresTo(uint32_t sourceBatchRowId, const ColumnIdMappingsContainer& mappings, Batch* targetBatch, uint32_t targetBatchRowId) const {
                for(auto& mapping : mappings.getMapping()){
                    // get and cast source column partition, the column partition from the build side referenced in 'probeResult'
                    const database::ColumnPartition<TABLE_PARTITION_SIZE>* sourceColumnPartition = _columnPartitions[mapping.first];
                    // copy the entry from the source column partition into the new target column partition
                    targetBatch->insertIntoColumnPartition(mapping.second, targetBatchRowId, sourceColumnPartition->getEntry(sourceBatchRowId));
                }
            }
    };


    template<uint32_t TABLE_PARTITION_SIZE>
    struct BatchTupleIdentifier{
        Batch<TABLE_PARTITION_SIZE>* _batch;
        uint16_t _batchRowId;

        BatchTupleIdentifier()
        : _batch(nullptr), _batchRowId(std::numeric_limits<uint16_t>::max()){
        }

        BatchTupleIdentifier(Batch<TABLE_PARTITION_SIZE>* batch, const uint16_t batchRowId)
        : _batch(batch), _batchRowId(batchRowId){
        }

        bool operator<(const BatchTupleIdentifier<TABLE_PARTITION_SIZE>& other) const {
            if(_batch == other._batch){
                return _batchRowId < other._batchRowId;
            }
            return _batch < other._batch;
        }

        bool operator==(const BatchTupleIdentifier<TABLE_PARTITION_SIZE>& other) const {
            return (_batch==other._batch) && (_batchRowId==other._batchRowId);
        }

        bool operator!=(const BatchTupleIdentifier<TABLE_PARTITION_SIZE>& other) const {
            return (_batch!=other._batch) || (_batchRowId!=other._batchRowId);
        }
    };
}

#endif
