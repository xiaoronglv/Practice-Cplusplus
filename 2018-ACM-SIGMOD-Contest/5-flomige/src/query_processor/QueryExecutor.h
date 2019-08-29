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

#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

#include "QueryOptimizer.h"
#include <sstream>


namespace query_processor {

    enum PipelineExecutionOrder {
        SIMPLE_ORDER,
        MEMORY_EFFICIENT_ORDER,
        ADOPTION_FRIENDLY_ORDER
    };


    struct ProcessingStats {
        std::string _printPlanTitle = "";
        std::string _printPlanFileName = "";
        uint64_t _estimatedCost = 0;
        uint64_t _trueCost = 0;
        // for adaptive execution we always re-enumerate (selective), even when the estimation
        // was right to propagate intermediate results in the plan table, therfore we distinguish
        // between necessary and actual re-optimizations and plan switch
        // if we re-enumerate with correct statistics we should get the same plan but it can also
        // be another plan with the same cost, therefore actual plan switches
        uint32_t _numberNecessaryReEnumerations = 0;
        uint32_t _numberActualReEnumerations = 0;
        uint32_t _numberNecessaryPlanSwitches = 0;
        uint32_t _numberActualPlanSwitches = 0;

        // with this constructor we just collect stats but do not print the plans as tikz strings
        ProcessingStats(){
        }

        // with this constructor we collect stats and print the plan steps as tikz strings to the passed file name
        ProcessingStats(const std::string& printPlanTitle, const std::string& printPlanFileName)
        : _printPlanTitle(printPlanTitle), _printPlanFileName(printPlanFileName){
        }
    };


    struct ProcessingTimes {
        double _totalProcessingTimeInMs = 0;
        double _initialOptimizationTimeInMs = 0;
        double _totalPipelineCreationTimeInMs = 0;
        double _totalPipelineExecutionTimeInMs = 0;
        double _totalReOptimizationTimeInMs = 0;
        double _totalAdaptiveOverheadInMs = 0;
    };


    template<uint32_t TABLE_PARTITION_SIZE>
    class ExecutionPlanPrinter{
        public:
            static void print(
                std::shared_ptr<FinalNode<TABLE_PARTITION_SIZE>> planTopNode,
                const std::string& title,
                const std::string& fileName,
                uint64_t executedOperatorsCount,
                uint64_t totalOperatorsCount
            ){
                // write latex code to files
                std::system("mkdir -p texs");
                std::ofstream outputFile;
                std::stringstream ss1;
                ss1 << std::setw(2) << std::setfill('0') << (executedOperatorsCount);
                std::stringstream ss2;
                ss2 << std::setw(2) << std::setfill('0') << (totalOperatorsCount);
                std::string fileNameSuffix = std::string("_")+ss1.str()+"_"+ss2.str();
                outputFile.open(std::string("texs/")+fileName+fileNameSuffix+".tex", std::ofstream::app);
                outputFile << planTopNode->template createLatexString(title);
            }
    };


    // TableMetaSimplified + JoinAttributeMetaSimplified

    template<
        uint32_t TABLE_PARTITION_SIZE,
        template<uint32_t, template<uint32_t, template<uint32_t> class,bool> class, template<uint32_t> class, bool > class OptimizerType,
        template<uint32_t, template<uint32_t> class,bool> class EnumeratorType,
        template<uint32_t> class PlanClassType,
        bool isPhysicalEnumeration,
        bool isAdaptiveExecution,
        PipelineExecutionOrder pipeExecutionOrder,
        bool printProcessingInfo,
        bool collectStats,
        bool collectTimes
    >
    class QueryExecutor {
        private:
            OptimizerType<TABLE_PARTITION_SIZE, EnumeratorType, PlanClassType, isPhysicalEnumeration> _optimizer;

        public:
            // constructor taking the optimizer with optimal plan and dynamic programming table as parameter
            QueryExecutor() {
            }


            // takes the optimal plan from the '_optimizer' member and executes it pipeline wise, has to be invoked in a
            // 'basis::TaskBase::execute()' context since it executes pipelines otherwise a runtime execption will be thrown
            std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> execute(
                const database::Database<TABLE_PARTITION_SIZE>& database,
                const std::vector<TableInput>& tablesInput,
                const std::vector<JoinAttributeInput>& innerEquiJoinsInput,
                const std::vector<query_processor::ProjectionMetaData>& projections,
                ProcessingStats* stats = nullptr,
                ProcessingTimes* times = nullptr,
                uint64_t heuristicsUncertaintyValue = 0
            ){
                // start overall processing time measuring and initial optimization time measuring
                std::chrono::time_point<std::chrono::system_clock> processingStart;
                std::chrono::time_point<std::chrono::system_clock> initialOptimizationStart;
                if(collectTimes){
                    processingStart = std::chrono::system_clock::now();
                    initialOptimizationStart = std::chrono::system_clock::now();
                }

                // optimize the query
                _optimizer.template optimize(database, tablesInput, innerEquiJoinsInput, projections);

                 // finish initial optimization time measuring and save value
                if(collectTimes){
                    std::chrono::duration<double> elapsedSeconds = (std::chrono::system_clock::now() - initialOptimizationStart);
                    times->_initialOptimizationTimeInMs = 1000 * elapsedSeconds.count();
                }

                // get the currently optimal plan from the optimizer
                std::shared_ptr<FinalNode<TABLE_PARTITION_SIZE>> planTopNode = _optimizer.getCurrentOptimalPlan();

                // to track how many operators we have already executed, since we are not going to re-optmize when less than two operators are left
                uint32_t totalOperatorsCount = _optimizer.getTotalOperatorsCount();
                uint32_t executedOperatorsCount = 0;

                // print initial plan
                if(printProcessingInfo){
                    std::cout << "initial plan:\n" << planTopNode->toString() << std::endl << std::endl;
                }

                // write latex string of initial plan
                if(collectStats){
                    if(stats->_printPlanFileName != ""){
                        ExecutionPlanPrinter<TABLE_PARTITION_SIZE>::print(planTopNode, stats->_printPlanTitle, stats->_printPlanFileName, executedOperatorsCount, totalOperatorsCount);
                    }
                }

                // for prints and stats, variable to store the latest plan string to detect plan switches
                std::string latestPlanString;
                if(collectStats || printProcessingInfo){
                    latestPlanString = planTopNode->toString();
                }
                // to keep track of all intermdiate result nodes we have added to the plan table, they contain shared pointers
                // to hash join build breakers that have to be set to nullptr after usage to be deallocated at the right time
                std::vector<std::pair<std::bitset<64>, std::shared_ptr<IntermediateResultNode<TABLE_PARTITION_SIZE>>>> setIntermediateResults;

                // execute the query pipeline wise, search for the next breaker corresponding to a certain execution strategy
                std::shared_ptr<BreakerNodeBase<TABLE_PARTITION_SIZE>> nextBreaker;
                if(pipeExecutionOrder == SIMPLE_ORDER){
                    nextBreaker = planTopNode->findNextPipelineSimpleOrder();
                }
                else if(pipeExecutionOrder == MEMORY_EFFICIENT_ORDER){
                    nextBreaker = planTopNode->findNextPipelineMemoryEfficientOrder();
                }
                else if(pipeExecutionOrder == ADOPTION_FRIENDLY_ORDER){
                    nextBreaker = planTopNode->findNextPipelineAdoptionFriendlyOrder();
                }
                else{
                    throw std::runtime_error("Undefined pipeline execution strategy");
                }
                while(nextBreaker != nullptr){

                    // pipeline creation
                    // start pipeline creation time measuring
                    std::chrono::time_point<std::chrono::system_clock> pipelineCreationStart;
                    if(collectTimes){
                        pipelineCreationStart = std::chrono::system_clock::now();
                    }

                    // create the pipeline pointer, pointing to null
                    std::shared_ptr<ExecutablePipeline<TABLE_PARTITION_SIZE>> pipeline(nullptr);
                    // add operators to pipeline
                    nextBreaker->addOperatorToPipeline(pipeline, executedOperatorsCount);

                    // finish pipeline creation time measuring
                    if(collectTimes){
                        std::chrono::duration<double> elapsedSeconds = (std::chrono::system_clock::now() - pipelineCreationStart);
                        times->_totalPipelineCreationTimeInMs += (1000 * elapsedSeconds.count());
                    }

                    // pipeline execution
                    // start pipeline execution time measuring
                    std::chrono::time_point<std::chrono::system_clock> pipelineExecutionStart;
                    if(collectTimes){
                        pipelineExecutionStart = std::chrono::system_clock::now();
                    }

                    // execute the pipeline
                    pipeline->startExecution();
                    pipeline->waitForExecution();

                    // deallocate operators e.g. hash join build breakers (hash tables) that are not needed anymore to save memory, flag for recursion start
                    nextBreaker->deallocateOperators(true);

                    // finish pipeline execution time measuring
                    if(collectTimes){
                        std::chrono::duration<double> elapsedSeconds = (std::chrono::system_clock::now() - pipelineExecutionStart);
                        times->_totalPipelineExecutionTimeInMs += (1000 * elapsedSeconds.count());
                    }

                    // print
                    if(printProcessingInfo){
                        // pipeline->printOperatorsValidRowCount();
                        std::cout << "total executed operators till now - " << executedOperatorsCount << "/" << totalOperatorsCount << ":" << std::endl;
                    }

                    // adaptive extension
                    if(isAdaptiveExecution){

                        // start adaption overhead time measuring
                        std::chrono::time_point<std::chrono::system_clock> adaptiveExecutionStart;
                        if(collectTimes){
                            adaptiveExecutionStart = std::chrono::system_clock::now();
                        }

                        // retrieve executed plan class id and estimated statistics
                        std::bitset<64> executedPlanClassId;
                        uint64_t estimatedCardinality;
                        nextBreaker->getRuntimeFeedback(executedPlanClassId, estimatedCardinality);

                        // re-optimization window, ensure that we don't re-optimize on base tables,
                        if(executedPlanClassId.count() > 0){
                            // and that we have at least two more operators that were not yet executed
                            if(totalOperatorsCount >= 2 && executedOperatorsCount <= totalOperatorsCount-2){

                                // retrieve executed intermediate result
                                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> intermediateResultBreaker;
                                nextBreaker->getIntermediateResult(intermediateResultBreaker);

                                // for the full adaptive execution, we always re-optimize/re-enumerate, even when the estimation was right. this is
                                // necessary because of the selective re-enumeration. if we just link intermediate results in the plan table and do
                                // not re-enumerate in case of right estimation, then this intermediate result is not necessarily considered in the
                                // following plan classes, i.e., it might be lost in a selective re-enumeration later. we could also re-enumerate only
                                // at estimation errors, but as a consequence we then always have to do a full re-enumeration, i.e. no selective
                                // re-enumeration, since there might be intermediate results in the plan that have not been propagated in the plan
                                // class yet. since estimation errors occur much much more often than right estimations, we prefer the selective
                                // re-enumeration and therefore always re-optimize, even if the estimation was right. again if the estimation was right
                                // and we re-enumerate, we will not find a better plan, but the intermediate result nodes are considered

                                // re-optimization
                                // start re-optimization time measuring
                                std::chrono::time_point<std::chrono::system_clock> reOptimizationStart;
                                if(collectTimes){
                                    reOptimizationStart = std::chrono::system_clock::now();
                                }

                                // re-optimize, update planTopNode
                                _optimizer.reoptimize(executedPlanClassId, intermediateResultBreaker, projections, (heuristicsUncertaintyValue==0));
                                planTopNode = _optimizer.getCurrentOptimalPlan();

                                // finish re-optimization time measuring
                                if(collectTimes){
                                    std::chrono::duration<double> elapsedSeconds = (std::chrono::system_clock::now() - reOptimizationStart);
                                    times->_totalReOptimizationTimeInMs += (1000 * elapsedSeconds.count());
                                }

                                // print or stats
                                if(printProcessingInfo || collectStats){
                                    std::string newPlanString = planTopNode->toString();
                                    // print
                                    if(printProcessingInfo){
                                        auto it = _optimizer.findPlanClass(executedPlanClassId);
                                        std::cout
                                            << "reoptimization, executed sub plan: " << it->second.getOptimalPlan()->toString()
                                            << "\nestimated cardinality: " << estimatedCardinality
                                            << ", true cardinality: " << intermediateResultBreaker->getValidRowCount()
                                            << ", new plan:\n" << planTopNode->toString();
                                        if(newPlanString != latestPlanString){
                                            std::cout << " (plan switch)" << std::endl;
                                            nextBreaker->setPlanSwitched(true);
                                        }
                                        else{
                                            std::cout << " (no plan switch)" << std::endl;
                                        }
                                    }
                                    // update stats
                                    if(collectStats){
                                        // for adaptive execution we always re-enumerate (selective), even when the estimation was right
                                        stats->_numberActualReEnumerations++;
                                        if(estimatedCardinality != intermediateResultBreaker->getValidRowCount()){
                                            stats->_numberNecessaryReEnumerations++;
                                        }
                                        if(newPlanString != latestPlanString){
                                            stats->_numberActualPlanSwitches++;
                                            if(estimatedCardinality != intermediateResultBreaker->getValidRowCount()){
                                                stats->_numberNecessaryPlanSwitches++;
                                            }
                                            // write latex string of new plan
                                            if(stats->_printPlanFileName != ""){
                                                ExecutionPlanPrinter<TABLE_PARTITION_SIZE>::print(
                                                    planTopNode, stats->_printPlanTitle, stats->_printPlanFileName, executedOperatorsCount, totalOperatorsCount);
                                            }
                                        }
                                    }
                                    // update latest plan string variable
                                    latestPlanString = newPlanString;
                                }
                            }
                            else{
                                // print
                                if(printProcessingInfo){
                                    auto it = _optimizer.findPlanClass(executedPlanClassId);
                                    std::cout
                                        << "no reopt less than 2, executed sub plan: " << it->second.getOptimalPlan()->toString()
                                        << "\nestimated cardinality: " << estimatedCardinality
                                        << ", true cardinality: - "
                                        << ", new plan:\n" << planTopNode->toString()
                                        << " (no plan switch)"
                                        << std::endl;
                                }
                            }
                        }
                        // else{
                        //     // print
                        //     if(printProcessingInfo){
                        //         auto it = _optimizer.findPlanClass(executedPlanClassId);
                        //         std::cout
                        //             << "no reopt after base, executed sub plan: " << it->second.getOptimalPlan()->toString()
                        //             << "\nestimated cardinality: " << estimatedCardinality
                        //             << ", true cardinality: - "
                        //             << ", new plan:\n" << planTopNode->toString()
                        //             << " (no plan switch)"
                        //             << std::endl;
                        //     }
                        // }

                        // finish adaptive execution overhead time measuring
                        if(collectTimes){
                            std::chrono::duration<double> elapsedSeconds = (std::chrono::system_clock::now() - adaptiveExecutionStart);
                            times->_totalAdaptiveOverheadInMs += (1000 * elapsedSeconds.count());
                        }
                    }

                    // search for the next deepest unexecuted pipeline
                    if(pipeExecutionOrder == SIMPLE_ORDER){
                        nextBreaker = planTopNode->findNextPipelineSimpleOrder();
                    }
                    else if(pipeExecutionOrder == MEMORY_EFFICIENT_ORDER){
                        nextBreaker = planTopNode->findNextPipelineMemoryEfficientOrder();
                    }
                    else if(pipeExecutionOrder == ADOPTION_FRIENDLY_ORDER){
                        nextBreaker = planTopNode->findNextPipelineAdoptionFriendlyOrder();
                    }
                    else{
                        throw std::runtime_error("Undefined pipeline execution strategy");
                    }

                    // print
                    if(printProcessingInfo){
                        std::cout << std::endl;
                    }

                    // finish overall processing time measuring and save value
                    if(collectTimes){
                        std::chrono::duration<double> elapsedSeconds = (std::chrono::system_clock::now() - processingStart);
                        times->_totalProcessingTimeInMs = 1000 * elapsedSeconds.count();
                    }
                }

                // ensure that all operators were executed
                if(executedOperatorsCount != totalOperatorsCount){
                    throw std::runtime_error("Detected mismatch in number of executed operators");
                }

                // update stats
                if(collectStats){
                    stats->_estimatedCost = planTopNode->getCost();
                    stats->_trueCost = planTopNode->getTrueCost();
                    // write latex string of final plan
                    if(stats->_printPlanFileName != ""){
                        std::string printPlanFileNameSuffix = std::string("_")+std::to_string(executedOperatorsCount)+"_"+std::to_string(totalOperatorsCount);
                        ExecutionPlanPrinter<TABLE_PARTITION_SIZE>::print(planTopNode, stats->_printPlanTitle, stats->_printPlanFileName, executedOperatorsCount, totalOperatorsCount);
                    }
                }

                // std::cout << "everything should be deallocated now!" << std::endl;

                // return the final breaker
                return planTopNode->getBreaker();
            }

            const auto& getOptimizer() const {
                return _optimizer;
            }

    };


    template<uint32_t TABLE_PARTITION_SIZE>
    using SimpleQueryExecutor = QueryExecutor<
        TABLE_PARTITION_SIZE,
        QueryOptimizerAdaptive,
        DPSizeEnumeratorAdaptive,
        PlanClassAdaptive,
        // TODO
        // this should be a 'QueryOptimizer', 'DPSizeEnumerator' and 'PlanClass' but the executor in the adaptive case does some invocations to the
        // query optimizer that are not in the regular optimizer although the adaptive execution is not enable by template parameter
        false,
        false,
        MEMORY_EFFICIENT_ORDER,
        false,
        false,
        false
    >;


    template<uint32_t TABLE_PARTITION_SIZE>
    using AdaptiveQueryExecutor = QueryExecutor<
        TABLE_PARTITION_SIZE,
        QueryOptimizerAdaptive,
        DPSizeEnumeratorAdaptive,
        PlanClassAdaptive,
        true,
        true,
        MEMORY_EFFICIENT_ORDER,
        false,
        false,
        false
    >;

}

#endif
