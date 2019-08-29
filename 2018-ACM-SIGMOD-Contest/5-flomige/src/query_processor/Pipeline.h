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

#ifndef PIPELINE_H
#define PIPELINE_H

#include "PipelineStarter.h"
#include "FilterOperator.h"
#include "HashJoinFactory.h"
#include "PipelineBreaker.h"

#include <limits>

namespace query_processor {

    // pipeline base class that contains an add-function for each operator, the current columns we work on, as well as pointers for breaker and starter
    template<uint32_t TABLE_PARTITION_SIZE>
    class PipelineBase{
        protected:
            // contains the current pipeline columns with current id, current name, and column pointer, it allows to search for columns by name
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _currentColumns;

            // pointers to pipeline starter and breaker
            std::shared_ptr<PipelineStarterBase<TABLE_PARTITION_SIZE>> _starter = nullptr;
            std::shared_ptr<PipelineBreakerBase<TABLE_PARTITION_SIZE>> _breaker = nullptr;

            // points to the latest operator that was added to this pipe
            std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> _latestOperator = nullptr;

            // checks if all preconditions are fulfilled before add an operator
            void addOperatorCheck(){
                // operators can be only set no breaker set yet
                if(_breaker!=nullptr){
                    throw std::runtime_error("Impossible to add operator to pipeline since pipeline breaker already set");
                }
            }
            // invoked to add any kind an operator to the linked list of operators in the this pipeline
            void addOperator(std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> oper){
                // in case this is the first operator after a starter
                if(_latestOperator==nullptr){
                    // set first operator in '_latestStarter'
                    _starter->setFirstOperator(oper);
                }
                // in case we already have operators
                else{
                    // set next operator in '_latestOperator'
                    _latestOperator->setNextOperator(oper);
                }
                // set '_latestOperator'
                _latestOperator=oper;
            }

            // invoked to add any kind of pipeline breaker to the end of the linked list of operators
            void addBreaker(std::shared_ptr<PipelineBreakerBase<TABLE_PARTITION_SIZE>> breaker){
                // create a new breaker and add it as operator
                addOperator(std::static_pointer_cast<OperatorBase<TABLE_PARTITION_SIZE>>(breaker));
                _breaker=breaker;
            }

            // basically adds the columns of the other join side to this pipeline's '_currentColumns',
            // also creates a mapping from the old column id to the new column id in this pipeline
            void addJoinColumns(
                std::vector<std::string>& probeColumnsCurrentNames,
                std::vector<std::string>& buildColumnsCurrentNames,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns,
                ColumnIdMappingsContainer& columnIdMappings
            ){
                // check if all build join columns were distinct or not
                bool isDistinctAfterJoin = true;
                std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& columns = buildPipeColumns.getColumns();
                for(uint32_t oldColumnId=0; oldColumnId<columns.size(); oldColumnId++){
                    // iterate over all columns to find if it is a join column
                    for (uint32_t newColumnId = 0; newColumnId < buildColumnsCurrentNames.size(); ++newColumnId) {
                        // check if the current build column is a join column
                        if (columns[oldColumnId].isName(buildColumnsCurrentNames[newColumnId])) {
                            // check if one of the join columns were non distinct
                            if (!columns[oldColumnId]._isDistinct) {
                                isDistinctAfterJoin = false;
                            }
                        }
                    }
                }
                // add columns of build pipeline/build breaker into probe pipeline, this has to take place before the join operator is created
                for(uint32_t oldColumnId=0; oldColumnId<columns.size(); oldColumnId++){
                    // check if the current column is a join column (then we don't have to copy it)
                    bool isJoinColumn = false;
                    for (uint32_t newColumnId = 0; newColumnId < buildColumnsCurrentNames.size(); ++newColumnId) {
                        // check if the current build column is a join column
                        if (columns[oldColumnId].isName(buildColumnsCurrentNames[newColumnId])) {
                            // add additional column names to the current probe column
                            _currentColumns.addAdditionalColumnNames(probeColumnsCurrentNames[newColumnId], columns[oldColumnId]._names,
                                columns[oldColumnId]._isProjectionColumn, columns[oldColumnId]._projectionCount, columns[oldColumnId]._operatorCount,
                                columns[oldColumnId]._min, columns[oldColumnId]._max, columns[oldColumnId]._isDistinct);
                            isJoinColumn = true;
                            break;
                        }
                    }
                    // if it is wasn't a join column, then we have to add it
                    if (!isJoinColumn) {
                        // add column to this pipe
                        uint32_t newColumnId = _currentColumns.addNewColumn(columns[oldColumnId]._names, columns[oldColumnId]._column,
                            columns[oldColumnId]._isProjectionColumn, columns[oldColumnId]._projectionCount, columns[oldColumnId]._operatorCount,
                            columns[oldColumnId]._min, columns[oldColumnId]._max, columns[oldColumnId]._isDistinct);
                        columnIdMappings.addRawColumnIdMapping(oldColumnId, newColumnId);
                    }
                }
                // if the join was no distinct, we set all columns to non distinct
                if (!isDistinctAfterJoin) {
                    _currentColumns.setAllColumnsNonDistinct();
                }
            }

        public:
            virtual ~PipelineBase(){
            }

            // projection operator that only modifies the order and the name of the columns in the pipeline and applies the order to its pushed batches
            // is not doing distincts or calculations
            class ProjectionOperator : public OperatorBase<TABLE_PARTITION_SIZE> {
                private:
                    PipelineBase& _pipe;
                    std::shared_ptr<BaseTableStarter<TABLE_PARTITION_SIZE>> _baseTableStarter;
                    PipelineColumnsContainer<TABLE_PARTITION_SIZE> _oldColumns;
                    std::vector<uint32_t> _oldColumnIds;  // _oldColumnIds[newColumnId]

                public:
                    // constructor takes a pipeline reference to modify its '_currentColumns' container, if this projection is the first operator after a base table starter,
                    // we expect a pointer to the starter to modify its initial columns, which avoids creating a full batch and project it immediately afterwards again
                    ProjectionOperator(PipelineBase<TABLE_PARTITION_SIZE>& pipe, std::shared_ptr<BaseTableStarter<TABLE_PARTITION_SIZE>> baseTableStarter, RuntimeStatistics* runtimeStatistics = nullptr)
                        : OperatorBase<TABLE_PARTITION_SIZE>(runtimeStatistics), _pipe(pipe), _baseTableStarter(baseTableStarter) {
                            // save the old columns container in this operator and set the one in the pipeline empty
                            // at that point there is no column projected anymore, since we are in the constructor '_oldColumns' is always empty
                            pipe._currentColumns.swap(_oldColumns);
                            // in case this is an initial projection on a base table -> baseTableStarter in not null, otherwise it is null
                        }

                    // after adding a projection operator to the pipeline, there are no projected columns anymore, so this function has to be invoked to add the columns
                    // again to the pipe using there current name, the order of invoking this function (adding columns) represents the column order after this operator.
                    // a single column can be projected multiple times but will need an alias since column names are unique
                    void addUsedColumns(){
                        // TODO what happens when this is called after another operator was already added to the pipeline
                        // only allow this call when next operator of this operator is still empty
                        // TODO create a test case for that

                        // get all old used columns
                        std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& oldUsedColumns = _oldColumns.getColumns();
                        for (uint32_t oldColumnId = 0; oldColumnId < oldUsedColumns.size(); ++oldColumnId) {
                            NameColumnStruct<TABLE_PARTITION_SIZE>& usedColumn = oldUsedColumns[oldColumnId];
                            // add it to the current columns if the operator count is bigger than zero
                            if (usedColumn._operatorCount > 0) {
                                _pipe._currentColumns.addNewColumn(usedColumn._names, usedColumn._column, usedColumn._isProjectionColumn,
                                    usedColumn._projectionCount, usedColumn._operatorCount, usedColumn._min, usedColumn._max, false);
                                _oldColumnIds.push_back(oldColumnId);
                            }
                            // add the column id to the pipeline's initial column ids if this is the initial projection
                            // in case this is an initial projection on a base table -> baseTableStarter in not null, otherwise it is null
                            // if(_baseTableStarter != nullptr){
                            //     // _baseTableStarter->addInitialColumnId(oldColumnId);
                            // }
                        }
                    }

                    // implementation of the operator base push function, basically just reorders the column partitions in the batch to correspond to the new column order
                    void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                        // if '_baseTableStarter' is null, we do the actual work since this is not the first projection on a base table
                        if(_baseTableStarter == nullptr){
                            // do the actual work
                            batch->reArrangeColumnPartitions(_oldColumnIds);
                        }
                        // in any case we push the tuple to the next operator
                        OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                        OperatorBase<TABLE_PARTITION_SIZE>::_nextOperator->push(batch, workerId);
                    }
            };

            // adds a projection operator to this pipeline, after adding such an operator no column is projected anymore, so 'addColumn' of the returned projection
            // operator has to be invoked to project columns again
            std::shared_ptr<ProjectionOperator> addProjectionOperator(RuntimeStatistics* runtimeStatistics = nullptr){
                // check preconditions
                addOperatorCheck();
                // check if this is an initial projection on a base table starter, i.e. current starter is a BaseTableStarter and its first operator is not yet set
                // if yes -> 'baseTableStarter' will be set, otherwise -> 'baseTableStarter' will be null
                std::shared_ptr<BaseTableStarter<TABLE_PARTITION_SIZE>> baseTableStarter = std::dynamic_pointer_cast<BaseTableStarter<TABLE_PARTITION_SIZE>>(_starter);
                if(baseTableStarter != nullptr){
                    if(baseTableStarter->isFirstOperatorSet()){
                        // set 'baseTableStarter' to null
                        baseTableStarter = nullptr;
                    }
                }
                // create initial projection object
                auto projection = std::make_shared<typename PipelineBase<TABLE_PARTITION_SIZE>::ProjectionOperator>(*this, baseTableStarter, runtimeStatistics);
                // add it to the pipe
                addOperator(std::static_pointer_cast<OperatorBase<TABLE_PARTITION_SIZE>>(projection));
                // TODO: check if we want to do it here
                // update the columns
                projection->addUsedColumns();
                return projection;
            }

            // adds a less filter to this pipeline, expecting the name of the column we filter on and the less filter
            void addLessFilterOperator(
                std::string filterColumnCurrentName,
                uint64_t lessValueCellBase,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // check preconditions
                addOperatorCheck();
                // search for the column in the columns container
                basis::Utilities::validName(filterColumnCurrentName);
                ColumnIdColumnStruct<TABLE_PARTITION_SIZE> res = _currentColumns.find(filterColumnCurrentName);
                uint32_t filterColumnPipelineId = res._pipelineColumnId;
                // create new less filter operator
                addOperator(std::make_shared<LessFilterOperator<TABLE_PARTITION_SIZE>>(_currentColumns, filterColumnPipelineId, lessValueCellBase, runtimeStatistics));
                // update the operator count for this column
                _currentColumns.decreaseOperatorCount(filterColumnCurrentName, 1);
                // update the column statistic
                _currentColumns.addFilterStatistic(filterColumnCurrentName, 0, lessValueCellBase);
                return;
            }

            // adds a equal filter to this pipeline, expecting the name of the column we filter on and the equal filter
            void addEqualFilterOperator(
                std::string filterColumnCurrentName,
                uint64_t equalValueCellBase,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // check preconditions
                addOperatorCheck();
                // search for the column in the columns container
                basis::Utilities::validName(filterColumnCurrentName);
                ColumnIdColumnStruct<TABLE_PARTITION_SIZE> res = _currentColumns.find(filterColumnCurrentName);
                uint32_t filterColumnPipelineId = res._pipelineColumnId;
                // create new equal filter operator
                addOperator(std::make_shared<EqualFilterOperator<TABLE_PARTITION_SIZE>>(_currentColumns, filterColumnPipelineId, equalValueCellBase, runtimeStatistics));
                // update the operator count for this column
                _currentColumns.decreaseOperatorCount(filterColumnCurrentName, 1);
                // update the column statistic
                _currentColumns.addFilterStatistic(filterColumnCurrentName, equalValueCellBase, equalValueCellBase);
                return;
            }

            // adds a greater filter to this pipeline, expecting the name of the column we filter on and the greater filter
            void addGreaterFilterOperator(
                std::string filterColumnCurrentName,
                uint64_t greaterValueCellBase,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // check preconditions
                addOperatorCheck();
                // search for the column in the columns container
                basis::Utilities::validName(filterColumnCurrentName);
                ColumnIdColumnStruct<TABLE_PARTITION_SIZE> res = _currentColumns.find(filterColumnCurrentName);
                uint32_t filterColumnPipelineId = res._pipelineColumnId;
                // create new greater filter operator
                addOperator(std::make_shared<GreaterFilterOperator<TABLE_PARTITION_SIZE>>(_currentColumns, filterColumnPipelineId, greaterValueCellBase, runtimeStatistics));
                // update the operator count for this column
                _currentColumns.decreaseOperatorCount(filterColumnCurrentName, 1);
                // update the column statistic
                _currentColumns.addFilterStatistic(filterColumnCurrentName, greaterValueCellBase, std::numeric_limits<uint64_t>::max());
                return;
            }

            // HASH JOINS:
            // there are basically to kinds of hash joins: early and late. early hash joins require a special breaker ('HashJoinEarlyBreakerBase') that
            // does the partitioning when the tuples are pushed to the breaker. therefore they have to know the build column in advance. late hash joins
            // enable to decide the build column after all tuples were pushed to the (default) breaker. a late hash join starts with reading all tuples from
            // the default breaker and partitioning them. performance-wise an early partitioning is much faster compared to a late partitioning since all
            // the batches are still in the cache when we do the partitioning.
            // it is recommend to always speculate on the build column and do an early partitiong. once it figures out that we choose the wrong build column,
            // we put the 'HashJoinEarlyBreakerBase' into a late hash join specifying a different build column. this works since each 'HashJoinEarlyBreakerBase'
            // is also a 'DefaultBreaker'

            // adds a hash join (late version) to this pipeline, expecting a pointer to the default breaker of the hash join build side, the name of the build
            // column there, and if the build column is distinct, and the name of the probe column, 'Late' indicates that the decision on which column the hash
            // table is build can be done postponed until this point i.e. there is no dedicated build breaker necessary that already partitioned the build side
            // see also HASH JOINS
            void addHashJoinLateOperator(
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> buildBreaker,
                std::vector<std::string> buildColumnsCurrentNames,
                std::vector<std::string> probeColumnsCurrentNames,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // check preconditions
                addOperatorCheck();
                // search for the probe columns in this pipe's columns container
                std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>> probeColumnsPointers;
                std::vector<uint32_t> probeColumnsPipelineIds;
                for(std::string& probeColumnCurrentName : probeColumnsCurrentNames){
                    basis::Utilities::validName(probeColumnCurrentName);
                    ColumnIdColumnStruct<TABLE_PARTITION_SIZE> resProbe = _currentColumns.find(probeColumnCurrentName);
                    probeColumnsPointers.push_back(resProbe._column);
                    probeColumnsPipelineIds.push_back(resProbe._pipelineColumnId);
                }
                // consume columns container of build pipeline/build breaker
                PipelineColumnsContainer<TABLE_PARTITION_SIZE> buildPipeColumns;
                buildBreaker->consumeTempedColumnsContainer(buildPipeColumns);
                // search for the build columns in this pipe's columns container
                std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>> buildColumnsPointers;
                std::vector<uint32_t> buildColumnsPipelineIds;
                for(std::string& buildColumnCurrentName : buildColumnsCurrentNames){
                    basis::Utilities::validName(buildColumnCurrentName);
                    // it is important to search in  'buildPipeColumns' and not in '_current columns'!!!
                    ColumnIdColumnStruct resProbe = buildPipeColumns.find(buildColumnCurrentName);
                    buildColumnsPointers.push_back(resProbe._column);
                    buildColumnsPipelineIds.push_back(resProbe._pipelineColumnId);
                }
                // take a copy of the current columns
                // TODO only if build side not distinct necessary
                PipelineColumnsContainer<TABLE_PARTITION_SIZE> probeSideColumnsOnly(_currentColumns);
                // add columns of build pipeline/build breaker into probe pipeline, this has to take place before the join operator is created
                ColumnIdMappingsContainer columnIdMappings;
                addJoinColumns(probeColumnsCurrentNames, buildColumnsCurrentNames, buildPipeColumns, columnIdMappings);
                // update the operator count for this column
                for (std::string& buildColumnName : buildColumnsCurrentNames) {
                    _currentColumns.decreaseOperatorCount(buildColumnName, 1);
                }
                for (std::string& probeColumnName : probeColumnsCurrentNames) {
                    _currentColumns.decreaseOperatorCount(probeColumnName, 1);
                }
                // create and add hash join operator
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> hashJoin
                    = HashJoinLateOperatorFactory<TABLE_PARTITION_SIZE>::createInstance(
                        probeColumnsPointers,
                        buildColumnsPointers,
                        _currentColumns,
                        probeSideColumnsOnly,
                        columnIdMappings,
                        buildPipeColumns,
                        probeColumnsPipelineIds,
                        buildBreaker,
                        buildColumnsPipelineIds,
                        runtimeStatistics
                    );
                addOperator(hashJoin);
            }

            // adds a breaker to this pipeline which already partitions the input for a hash join, it expects the build column's name, a bit indicating if the build
            // side is distinct, and an estimation of how many tuples will be pushed (necesary to decide about the partions count)
            std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> addHashJoinEarlyBreaker(
                std::vector<std::string> buildColumnsCurrentNames,
                uint64_t estimatedBuildSideCardinality,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // check preconditions
                addOperatorCheck();
                // search for the build columns in this pipe's columns container
                std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>> buildColumnsPointers;
                std::vector<uint32_t> buildColumnsPipelineIds;
                for(std::string& buildColumnCurrentName : buildColumnsCurrentNames){
                    basis::Utilities::validName(buildColumnCurrentName);
                    ColumnIdColumnStruct res = _currentColumns.find(buildColumnCurrentName);
                    buildColumnsPointers.push_back(res._column);
                    buildColumnsPipelineIds.push_back(res._pipelineColumnId);
                }
                // invoke the factory to create the breaker
                std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> breaker = HashJoinEarlyBreakerFactory<TABLE_PARTITION_SIZE>::createInstance(
                    _currentColumns, buildColumnsPointers, buildColumnsPipelineIds, buildColumnsCurrentNames, estimatedBuildSideCardinality, runtimeStatistics);
                addBreaker(breaker);
                return breaker;
            }

            // adds a hash join (early version) to this pipeline, expecting a pointer to a dedicated build breaker ('HashJoinEarlyBreakerBase'), which already
            // contains the input partitioned on the build column, furthermore this call requires the name on the probe column in this pipeline
            // see also HASH JOINS
            void addHashJoinEarlyOperator(
                std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> buildBreaker,
                std::vector<std::string> buildColumnsCurrentNames,
                std::vector<std::string> probeColumnsCurrentNames,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // check preconditions
                addOperatorCheck();
                // search for the probe columns in this pipe's columns container
                std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>> probeColumnsPointers;
                std::vector<uint32_t> probeColumnsPipelineIds;
                for(std::string& probeColumnCurrentName : probeColumnsCurrentNames){
                    basis::Utilities::validName(probeColumnCurrentName);
                    ColumnIdColumnStruct resProbe = _currentColumns.find(probeColumnCurrentName);
                    probeColumnsPointers.push_back(resProbe._column);
                    probeColumnsPipelineIds.push_back(resProbe._pipelineColumnId);
                }
                // consume columns container of build pipeline/build breaker
                PipelineColumnsContainer<TABLE_PARTITION_SIZE> buildPipeColumns;
                buildBreaker->consumeTempedColumnsContainer(buildPipeColumns);
                // take a copy of the current columns
                // TODO only if build side not distinct necessary
                PipelineColumnsContainer<TABLE_PARTITION_SIZE> probeSideColumnsOnly(_currentColumns);
                // add columns of build pipeline/build breaker into probe pipeline, this has to take place before the join operator is created
                ColumnIdMappingsContainer columnIdMappings;
                addJoinColumns(probeColumnsCurrentNames, buildColumnsCurrentNames, buildPipeColumns, columnIdMappings);
                // update the operator count for this column
                for (std::string& buildColumnName : buildColumnsCurrentNames) {
                    _currentColumns.decreaseOperatorCount(buildColumnName, 1);
                }
                for (std::string& probeColumnName : probeColumnsCurrentNames) {
                    _currentColumns.decreaseOperatorCount(probeColumnName, 1);
                }
                // create and add the operator
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> hashJoin
                    = HashJoinEarlyOperatorFactory<TABLE_PARTITION_SIZE>::createInstance(
                        probeColumnsPointers,
                        _currentColumns,
                        probeSideColumnsOnly,
                        columnIdMappings,
                        buildPipeColumns,
                        probeColumnsPipelineIds,
                        buildBreaker,
                        runtimeStatistics
                    );
                addOperator(hashJoin);
            }


            // adds a simple pipeline breaker to this pipeline which just adds the pushed batches to a vector and provides an interface to pick them later e.g.
            // in a breaker-starter or query result
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> addDefaultBreaker(RuntimeStatistics* runtimeStatistics = nullptr){
                // check preconditions
                addOperatorCheck();
                // create and add operator
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> breaker(std::make_shared<DefaultBreaker>(_currentColumns, runtimeStatistics));
                addBreaker(breaker);
                return breaker;
            }

            // adds a simple projections pipeline breaker to this pipeline ...
            std::shared_ptr<ProjectionBreaker<TABLE_PARTITION_SIZE>> addProjectionBreaker(std::vector<ProjectionMetaData>& projections, RuntimeStatistics* runtimeStatistics = nullptr){
                // check preconditions
                addOperatorCheck();
                // create and add operator
                std::shared_ptr<ProjectionBreaker<TABLE_PARTITION_SIZE>> breaker(std::make_shared<ProjectionBreaker<TABLE_PARTITION_SIZE>>(_currentColumns, projections, runtimeStatistics));
                addBreaker(breaker);
                return breaker;
            }

            // performs final steps after all batches have been pushed to the breaker, e.g. performing the final sort steps in a sort breaker
            void postExecutionSteps() {
                // run over the operators starting from this starters '_firstOperator' until the pipeline breaker and check
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> baseOperator = _starter->getFirstOperator();
                while(baseOperator != nullptr){
                    // invoke the required function on each operator we find
                    baseOperator->setRuntimeStatistics();
                    baseOperator = baseOperator->getNextOperator();
                }
                // also set the runtime statistics in the starter
                _starter->setRuntimeStatistics();
            }
            // TODO remove at some point including the corresponding callstack
            void printOperatorsValidRowCount() const {
                _starter->printOperatorsValidRowCount();
            }

    };


    // regular input struct for a column
    struct ColumnInput {
        std::string _columnPermanentName;
        uint32_t _operatorCount;
        bool _isProjectionColumn;
        uint32_t _projectionCount;

        ColumnInput(std::string columnPermanentName, bool isProjectionColumn)
         : _columnPermanentName(columnPermanentName), _operatorCount(1), _isProjectionColumn(isProjectionColumn), _projectionCount(isProjectionColumn) {
        }
    };


    // almost all pipelines are 'ExecutablePipeline' s, which means they have a 'TaskCreatingPipelineStarter' and they have a function to start the execution and wait
    // for the execution to finish
    template<uint32_t TABLE_PARTITION_SIZE>
    class ExecutablePipeline : public PipelineBase<TABLE_PARTITION_SIZE> {
        private:
            // indicates the state of this pipeline i.e. execution was started or not, if the execution was started then this points to the task group we have to wait on
            std::mutex _waitTaskGroupMutex;
            std::shared_ptr<basis::AsyncTaskGroup> _waitTaskGroup = nullptr;

            // invoked in constructors that create base table starters (e.g. 'FullTableStarter' or 'IndexTableStarter') to initialize the '_currentColumns' container with
            // all columns of a base table
            void initializeColumnsFromBaseTable(
                std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> baseTable,
                std::string& tableAlias,
                std::vector<ColumnInput>& usedColumns,
                std::vector<uint32_t>& usedColumnIds
            ){
                // make sure we have valid table alias
                if(tableAlias == ""){
                    tableAlias = baseTable->getName();
                }
                else{
                    basis::Utilities::validName(tableAlias);
                }
                // create a temporary column for each base table column
                // initiate the '_currentColumns' container based on the table's columns
                const std::vector<std::shared_ptr<const database::PermanentColumn<TABLE_PARTITION_SIZE>>>& columns = baseTable->getColumns();
                // run over the base table columns
                for(uint32_t columnId=0; columnId<columns.size(); columnId++){
                    // if usedColumns is empty, push column
                    if (usedColumns.empty()) {
                        // create a temporary column and add it using the original column name
                        PipelineBase<TABLE_PARTITION_SIZE>::_currentColumns.addNewColumn(
                            std::vector<std::string>{tableAlias+"."+columns[columnId]->getName()},
                            columns[columnId]->createTemporaryColumn(),
                            false, // no projection column
                            0, // zero projection count
                            std::numeric_limits<uint32_t>::max(), // max operator count
                            columns[columnId]->getLatestMinValue(),
                            columns[columnId]->getLatestMaxValue(),
                            columns[columnId]->isDistinct()
                        );
                        usedColumnIds.push_back(columnId);
                        continue;
                    }
                    // check if this is a used column in the query graph
                    for (ColumnInput& usedColumn : usedColumns) {
                        if (columns[columnId]->getName() == usedColumn._columnPermanentName) {
                            // create a temporary column and add it using the original column name
                            PipelineBase<TABLE_PARTITION_SIZE>::_currentColumns.addNewColumn(
                                std::vector<std::string>{tableAlias+"."+usedColumn._columnPermanentName},
                                columns[columnId]->createTemporaryColumn(),
                                usedColumn._isProjectionColumn,
                                usedColumn._projectionCount,
                                usedColumn._operatorCount,
                                columns[columnId]->getLatestMinValue(),
                                columns[columnId]->getLatestMaxValue(),
                                columns[columnId]->isDistinct()
                            );
                            usedColumnIds.push_back(columnId);
                            break;
                        }
                    }
                }
            }

        public:
            // creates a pipeline that starts on a base table using a 'FullTableStarter'
            ExecutablePipeline(
                std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> baseTable,
                std::string tableAlias,
                std::vector<ColumnInput> usedColumns,
                RuntimeStatistics* runtimeStatistics = nullptr
            ){
                // initialize pipeline column based on the base table
                std::vector<uint32_t> usedColumnIds;
                initializeColumnsFromBaseTable(baseTable, tableAlias, usedColumns, usedColumnIds);
                // create full table starter object and add it to this pipe
                PipelineBase<TABLE_PARTITION_SIZE>::_starter = std::shared_ptr<PipelineStarterBase<TABLE_PARTITION_SIZE>>( new FullTableStarter<TABLE_PARTITION_SIZE>(baseTable, usedColumnIds, runtimeStatistics) );
            }
            // creates a pipeline that starts on a pipeline breaker using a 'BreakerStarter'
            ExecutablePipeline(std::shared_ptr<PipelineBreakerBase<TABLE_PARTITION_SIZE>> breaker, RuntimeStatistics* runtimeStatistics = nullptr){
                // initialize pipeline columns
                breaker->consumeTempedColumnsContainer(PipelineBase<TABLE_PARTITION_SIZE>::_currentColumns);
                // create a breaker starter and add it to this pipe
                PipelineBase<TABLE_PARTITION_SIZE>::_starter = std::shared_ptr<PipelineStarterBase<TABLE_PARTITION_SIZE>>(new BreakerStarter<TABLE_PARTITION_SIZE>(
                    breaker, PipelineBase<TABLE_PARTITION_SIZE>::_currentColumns, runtimeStatistics));
            }


            // after all operators including a breaker are added to this pipe, this function starts an asynchronous execution of this pipe i.e. creates tasks, submits them
            // to the task scheduler, and returns. it enables a simultaneous execution of several pipelines. a pipeline can be executed only once
            void startExecution(){
                // ensure that the pipeline is not yet executed
                std::unique_lock<std::mutex> uLock(_waitTaskGroupMutex);
                if(_waitTaskGroup != nullptr){
                    throw std::runtime_error("Tried to execute already executed pipeline");
                }

                // ensure that we have a non-instant-starter and breaker i.e. we can execute the pipe
                std::shared_ptr<TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE>> starterTaskCreating
                 = std::dynamic_pointer_cast<TaskCreatingPipelineStarter<TABLE_PARTITION_SIZE>>(PipelineBase<TABLE_PARTITION_SIZE>::_starter);
                if(starterTaskCreating!=nullptr && PipelineBase<TABLE_PARTITION_SIZE>::_breaker!=nullptr){

                    // create the execution task group
                    std::shared_ptr<basis::AsyncTaskGroup> executionTaskGroup = basis::AsyncTaskGroup::createInstance();
                    // check if the instant temp operators in the pipe are ready for execution and
                    // invokes 'HashJoinOperatorBase::buildHashJoin()' which builds the hash tables for the hash join operators
                    PipelineBase<TABLE_PARTITION_SIZE>::_starter->pipelinePreExecutionPreparations();
                    // creates tasks for pushing batches through the pipe
                    starterTaskCreating->addExecutionTasks(executionTaskGroup);
                    // add flushing task groups which recursively trigger the pushing of consolidated batches that remained in the operators
                    // one task group per operator, one task per batch/worker
                    _waitTaskGroup = starterTaskCreating->addFlushTaskGroups(executionTaskGroup);
                    // start the execution on the first task group
                    executionTaskGroup->startExecution();
                }
                else if(starterTaskCreating==nullptr){
                    throw std::runtime_error("Unable to execute pipeline due to missing (non-instant) starter");
                }
                else{
                    throw std::runtime_error("Unable to execute pipeline due to missing breaker 1");
                }
            }
            // after 'startExecution()' was invoked to execute the pipeline in the background, this function can be invoked to wait for the execution to finish.
            // the invoking context is not sleeping or busy waiting but also processes tasks while waiting. in case a task failed before and after this function
            // is invoked, an exception will be thrown in this function
            void waitForExecution(){
                //
                if(_waitTaskGroup == nullptr){
                    throw std::runtime_error("Tried to wait on a pipeline that was not yet started");
                }
                _waitTaskGroup->wait();

                // TODO do this in 'startExecution' as additional task group after the flush task group
                PipelineBase<TABLE_PARTITION_SIZE>::postExecutionSteps();
            }

    };

}

#endif
