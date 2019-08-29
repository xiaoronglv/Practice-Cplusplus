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

#ifndef FILTER_OPERATOR_H
#define FILTER_OPERATOR_H

#include "Operator.h"

#include <iostream> // TODO remove

namespace query_processor {

    template<uint32_t TABLE_PARTITION_SIZE>
    class LessFilterOperator : public FlushingOperator<TABLE_PARTITION_SIZE> {

        private:
            uint32_t _filterColumnPipelineId;
            uint64_t _lessValue;

        public:
            LessFilterOperator(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns,
                uint32_t filterColumnPipelineId,
                uint64_t lessValue,
                RuntimeStatistics* runtimeStatistics)
            : FlushingOperator<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics), _filterColumnPipelineId(filterColumnPipelineId), _lessValue(lessValue) {
            }

            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                // fetch the column partition and their entries which we will read
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* batchColumn = batch->getColumnPartition(_filterColumnPipelineId);
                const std::array<uint64_t,TABLE_PARTITION_SIZE>& entries = batchColumn->getEntries();
                // run over the batch, check the condition and maybe invalidate the row
                for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                    if(entries[batchRowId] >= _lessValue){
                        batch->invalidateRow(batchRowId);
                    }
                }
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                FlushingOperator<TABLE_PARTITION_SIZE>::pushNextCheckDensity(batch, workerId, false);
            }
    };


    template<uint32_t TABLE_PARTITION_SIZE>
    class EqualFilterOperator : public FlushingOperator<TABLE_PARTITION_SIZE> {

        private:
            uint32_t _filterColumnPipelineId;
            uint64_t _equalValue;

        public:
            EqualFilterOperator(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns,
                uint32_t filterColumnPipelineId,
                uint64_t equalValue,
                RuntimeStatistics* runtimeStatistics)
            : FlushingOperator<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics), _filterColumnPipelineId(filterColumnPipelineId), _equalValue(equalValue) {
            }

            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                // fetch the column partition and their entries which we will read
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* batchColumn = batch->getColumnPartition(_filterColumnPipelineId);
                const std::array<uint64_t,TABLE_PARTITION_SIZE>& entries = batchColumn->getEntries();
                // run over the batch, check the condition and maybe invalidate the row
                for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                    if(entries[batchRowId] != _equalValue){
                        batch->invalidateRow(batchRowId);
                    }
                }
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                FlushingOperator<TABLE_PARTITION_SIZE>::pushNextCheckDensity(batch, workerId, false);
            }
    };


    template<uint32_t TABLE_PARTITION_SIZE>
    class GreaterFilterOperator : public FlushingOperator<TABLE_PARTITION_SIZE> {

        private:
            uint32_t _filterColumnPipelineId;
            uint64_t _greaterValue;

        public:
            GreaterFilterOperator(
                const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns,
                uint32_t filterColumnPipelineId,
                uint64_t greaterValue,
                RuntimeStatistics* runtimeStatistics)
            : FlushingOperator<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics),
              _filterColumnPipelineId(filterColumnPipelineId), _greaterValue(greaterValue) {
            }

            void push(std::shared_ptr<Batch<TABLE_PARTITION_SIZE>> batch, uint32_t workerId){
                // fetch the column partition and their entries which we will read
                const database::ColumnPartition<TABLE_PARTITION_SIZE>* batchColumn = batch->getColumnPartition(_filterColumnPipelineId);
                const std::array<uint64_t,TABLE_PARTITION_SIZE>& entries = batchColumn->getEntries();
                // run over the batch, check the condition and maybe invalidate the row
                for(uint32_t batchRowId=0; batchRowId<batch->getCurrentSize(); ++batchRowId){
                    if(entries[batchRowId] <= _greaterValue){
                        batch->invalidateRow(batchRowId);
                    }
                }
                OperatorBase<TABLE_PARTITION_SIZE>::addUpValidRowCount(batch->getValidRowCount(), workerId);
                FlushingOperator<TABLE_PARTITION_SIZE>::pushNextCheckDensity(batch, workerId, false);
            }
    };

}

#endif
