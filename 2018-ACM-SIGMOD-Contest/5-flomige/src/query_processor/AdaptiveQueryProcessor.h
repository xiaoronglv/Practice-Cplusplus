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

#ifndef ADAPTIVE_QUERY_PROCESSOR_H
#define ADAPTIVE_QUERY_PROCESSOR_H

#include "QueryExecutor.h"

namespace query_processor {


    template<
        class TableInputType,
        class InnerEquiJoinInputType,
        bool isPhysicalEnumeration,
        bool isAdaptiveExecution,
        PipelineExecutionOrder pipeExecutionOrder,
        bool printProcessingInfo,
        bool collectStats,
        bool collectTimes
    >
    class AdaptiveQueryProcessingTask : public basis::TaskBase {

        private:
            // executor member, which has an optimizer member
            QueryExecutor<
                QueryOptimizerAdaptive,
                DPSizeEnumeratorAdaptive,
                PlanClassAdaptive,
                TableMetaSimplified,
                JoinAttributeMetaSimplified,
                isPhysicalEnumeration,
                isAdaptiveExecution,
                pipeExecutionOrder,
                printProcessingInfo,
                collectStats,
                collectTimes
            > _executor;

            // query processing parameters
            const database::Database& _database;
            const std::vector<TableInputType>& _tablesInput;
            const std::vector<InnerEquiJoinInputType>& _innerEquiJoinsInput;
            ProcessingStats* _stats;
            ProcessingTimes* _times;
            uint64_t _heuristicsUncertaintyValue;

            // result
            std::shared_ptr<DefaultBreaker> _result;

        public:
            AdaptiveQueryProcessingTask(
                const database::Database& database,
                const std::vector<TableInputType>& tablesInput,
                const std::vector<InnerEquiJoinInputType>& innerEquiJoinsInput,
                ProcessingStats* stats,
                ProcessingTimes* times,
                uint64_t heuristicsUncertaintyValue) // TODO remove after comparative study
            : TaskBase(basis::OLAP_TASK_PRIO,0),
                _database(database),
                _tablesInput(tablesInput),
                _innerEquiJoinsInput(innerEquiJoinsInput),
                _stats(stats),
                _times(times),
                _heuristicsUncertaintyValue(heuristicsUncertaintyValue)
            {
                // avoid inconsistencies in statistics collection
                // if(collectStats && _stats == nullptr){
                //     throw std::runtime_error("Missing processing stats object");
                // }
                // if(!collectStats && _stats != nullptr){
                //     throw std::runtime_error("Passed processing stats object although no stats should be taken");
                // }
                // if(collectTimes && _times == nullptr){
                //     throw std::runtime_error("Missing processing times object");
                // }
                // if(!collectTimes && _times != nullptr){
                //     throw std::runtime_error("Passed processing times object although no times should be taken");
                // }
            }

            // implementation of task interface function
            void execute() {
                // optimize and execute the query
                _result = _executor.execute(_database, _tablesInput, _innerEquiJoinsInput, _stats, _times, _heuristicsUncertaintyValue);
            }

            //
            std::shared_ptr<DefaultBreaker> getResult() {
                return _result;
            }

            //
            std::string createLatexString(std::string caption) const {
                return _executor.getOptimizer().getCurrentOptimalPlan()->createLatexString(caption);
            }
    };

}

#endif
