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

#ifndef PIPELINE_BREAKER_H
#define PIPELINE_BREAKER_H

#include "Operator.h"

#include <atomic>

namespace query_processor {

    // base class for pipeline breakers containing interface to consume the batches and columns container of the the pipeline it broke to be used for a new pipeline
    // that starts on a breaker
    template<uint32_t TABLE_PARTITION_SIZE>
    class PipelineBreakerBase : public OperatorBase<TABLE_PARTITION_SIZE> {

        protected:
            PipelineColumnsContainer<TABLE_PARTITION_SIZE> _tempedPipeColumns;
            bool _isColumnsContainerConsumed=false;
            // other than the columns container, the batches have to be consumed after execution
            // in case of e.g. a hash join build breaker there could be simoultaneous consumers i.e. the hash join and a breaker starter
            std::mutex _areBatchesConsumedMutex;
            bool _areBatchesConsumed=false;

        public:
            PipelineBreakerBase(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, RuntimeStatistics* runtimeStatistics)
            : OperatorBase<TABLE_PARTITION_SIZE>(runtimeStatistics) {
                // the column name map can be swap from the pipeline since there are no more opations added after the breaker!
                _tempedPipeColumns.swap(currentPipeColumns);
            }

            virtual ~PipelineBreakerBase(){
            }

            // provides the temped columns container to the new pipeline, can be called only once
            void consumeTempedColumnsContainer(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& targetPipeColumns){
                // ensure that the target is empty
                // if(!targetPipeColumns.empty()){
                //     throw std::runtime_error("Target not empty while consuming pipeline breaker's columns container");
                // }
                // // this function can be called only once, it swaps the temped column map
                // if(_isColumnsContainerConsumed){
                //     throw std::runtime_error("Tried to consume a pipeline breaker that was already consumed (column map)");
                // }
                _isColumnsContainerConsumed=true;
                targetPipeColumns.swap(_tempedPipeColumns);
            }


            // provides the batches to the starter of the new pipeline, can be called only once
            virtual void consumeBatches(std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>>& target) = 0;
    };


    // a breaker for query results with applied materialization flags on columns and column partitions in batches
    template<uint32_t TABLE_PARTITION_SIZE>
    class DefaultBreaker : public PipelineBreakerBase<TABLE_PARTITION_SIZE> {
        protected:
            //
            std::vector<std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>>> _batches;

        public:
            //
            DefaultBreaker(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, RuntimeStatistics* runtimeStatistics)
            : PipelineBreakerBase<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics), _batches(basis::TaskScheduler::getWorkerCount()) {
            }

            //
            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                // just push batch to '_batches[workerId]', tuples are not copied
                _batches[workerId].push_back(batch);
                // operator statistics
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
            }

            //
            void consumeBatches(std::vector<std::shared_ptr<Batch<TABLE_PARTITION_SIZE>>>& target){
                // ensure that the target is empty
                // if(!target.empty()){
                //     throw std::runtime_error("Target not empty while consuming default breaker's batches");
                // }
                // concurrency possible in parallel query execution i.e. two breaker starter using that breaker and the corresponding pipelines are executed simoultaneously
                std::unique_lock<std::mutex> uLock(PipelineBreakerBase<TABLE_PARTITION_SIZE>::_areBatchesConsumedMutex);
                // ensure that the batches of this breaker are still available and not yet consumed
                // if(PipelineBreakerBase<TABLE_PARTITION_SIZE>::_areBatchesConsumed){
                //     throw std::runtime_error("Tried to consume a pipeline breaker that was already consumed (batches)");
                // }
                PipelineBreakerBase<TABLE_PARTITION_SIZE>::_areBatchesConsumed=true;
                // we basically have to copy the batches from this classes two dimensional container into the targets one dimensional container
                // determine batch count
                uint32_t batchCount=0;
                for(const auto& workerBatches : _batches){
                    batchCount += workerBatches.size();
                }
                // reserve the target vector
                target.reserve(batchCount);
                // copy the shared pointer of each batch in this breaker to the target
                for(auto& workerBatches : _batches){
                    for(auto& batch : workerBatches){
                        target.push_back(std::move(batch));
                        // TODO - maybe move the batch? or in genral find a better way!? -> see hash join build breaker as well
                        //batch = nullptr;
                    }
                }
            }
    };

    // selection meta data for a breaker
    struct ProjectionMetaData {
        std::string _aliasTableName;
        std::string _columnPermanentName;

        ProjectionMetaData(std::string aliasTableName, std::string columnPermanentName)
        : _aliasTableName(aliasTableName), _columnPermanentName(columnPermanentName) {
        }
    };


    // a projection breaker that writes the sum of projected columns into a vector
    template<uint32_t TABLE_PARTITION_SIZE>
    class ProjectionBreaker : public DefaultBreaker<TABLE_PARTITION_SIZE> {
        private:
            // columns for projection
            std::vector<ProjectionMetaData> _projections;
            // vector for checksums
            std::vector<std::vector<uint64_t>> _checksums; // checksum[projectionId][workerId]

        public:

            ProjectionBreaker(
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns,
                std::vector<ProjectionMetaData>& projections,
                RuntimeStatistics* runtimeStatistics
            )
            : DefaultBreaker<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics), _projections(projections),
               _checksums(projections.size(), std::vector<uint64_t>(basis::TaskScheduler::getWorkerCount(), 0))
            {
                uint32_t columnsOperatorCount = 0;
                uint32_t columnsProjectionCount = 0;
                std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& columns = PipelineBreakerBase<TABLE_PARTITION_SIZE>::_tempedPipeColumns.getColumns();
                for (auto& col : columns) {
                    columnsOperatorCount += col._operatorCount;
                    columnsProjectionCount += col._projectionCount;
                    if (col._operatorCount == 0) {
                        std::cerr << "WRONG OPERATOR COUNT 1" << std::endl;
                    }
                    if (!col._isProjectionColumn) {
                        std::cerr << "WRONG OPERATOR COUNT 2" << std::endl;
                    }
                }
                if (columnsOperatorCount != _projections.size()) {
                        std::cerr << "WRONG OPERATOR COUNT 3" << std::endl;
                }
                if (columnsOperatorCount != columnsProjectionCount) {
                        std::cerr << "WRONG OPERATOR COUNT 4:" << std::endl;
                }
            }


            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                // check if the checksums were already calculated by a previous join operator
                std::vector<uint64_t>& checksums = batch->getChecksums();
                if (!batch->isChecksumUsed()) {
                    // we have to calculate the checksums
                    // set the checksum for this batch to true
                    batch->setIsChecksumUsed(true);
                    // run over all projection columns and calculate the checksum
                    std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& columns = this->_tempedPipeColumns.getColumns();
                    for (uint32_t columnId = 0; columnId < columns.size(); ++columnId) {
                        if (columns[columnId]._isProjectionColumn) {
                            // get the column partition
                            const database::ColumnPartition<TABLE_PARTITION_SIZE>* checksumColumnPartition = batch->getColumnPartition(columnId);
                            // run over the tuples in the batch
                            for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                                // check if row is still valid
                                if(batch->isRowValid(batchRowId)){
                                    // update the checksum
                                    checksums[columnId] += checksumColumnPartition->getEntry(batchRowId);
                                }
                            }

                        }
                    }
                }
                // iterate over all 'relevant' columns from the batch and update the checksum
                for (uint32_t i = 0; i < _projections.size(); ++i) {
                    // search for the column id in the pipeline columns container
                    std::string columnName = _projections[i]._aliasTableName + "." + _projections[i]._columnPermanentName;
                    uint32_t columnId = this->_tempedPipeColumns.find(columnName)._pipelineColumnId;
                    _checksums[i][workerId] += checksums[columnId];
                }
            }

            void getChecksum(std::vector<uint64_t>& checksums) {
                checksums.resize(_checksums.size(), 0);
                for (uint32_t projectionId = 0; projectionId < _checksums.size(); ++projectionId) {
                    std::vector<uint64_t>& checksum = _checksums[projectionId];
                    for (uint64_t& workerChecksum : checksum) {
                        checksums[projectionId] += workerChecksum;
                    }
                }
            }
    };


    // a pipeline breaker that writes the pushed tuples (from the pushed batch) into a new batch
    template<uint32_t TABLE_PARTITION_SIZE>
    class ReMaterializingBreaker : public PipelineBreakerBase<TABLE_PARTITION_SIZE>, public TupleRematerializer<TABLE_PARTITION_SIZE> {
        public:
            //
            ReMaterializingBreaker(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, RuntimeStatistics* runtimeStatistics)
            : PipelineBreakerBase<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics), TupleRematerializer<TABLE_PARTITION_SIZE>(currentPipeColumns) {
            }

            //
            virtual ~ReMaterializingBreaker(){
            }
    };

}

#endif
