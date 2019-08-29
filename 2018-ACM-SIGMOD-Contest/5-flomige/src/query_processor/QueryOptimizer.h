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

#ifndef QUERY_OPTIMIZER_H
#define QUERY_OPTIMIZER_H

#include "Enumerator.h"

#include <iostream>
#include <algorithm> // std::min and std::max

// PRINTS
// #define PRINT_REOPTIMIZATION_INFO
// #define PRINT_RANGE_CALC_INFO
// #define PRINT_QUERY_INPUT

// OPTIMALITY RANGE CALCULATION ALGORITHM FEATURES
// #define RANGE_CALC_CONSIDER_ALL_ALTERNATIVES

// LOGICAL OR PHYSICAL ENUMERATION
#define IS_LOGICAL_ENUMERATION


namespace query_processor {

    // // class is specialiezed for different meta data types, i.e., simplified or profiles, depending on the overload there are create functions
    // // with different signatures for different input types, i.e., with stats or without
    // template<class TableMetaType, class JoinAttributeMetaType>
    // class MetaCreator{
    // };

    // class specialization for meta of simplified estimation, has two different create functions
    template<uint32_t TABLE_PARTITION_SIZE>
    class MetaCreator{

        // TODO: eliminate notNullCount

        private:
            // returns the estimated join selectivity according to the min value, max value and cardinality of the left and right relation
            static double estimateJoinSelectivity(
                uint64_t leftMin,
                uint64_t leftMax,
                uint64_t leftNotNullCount,
                uint64_t leftCardinality,
                uint64_t rightMin,
                uint64_t rightMax,
                uint64_t rightNotNullCount,
                uint64_t rightCardinality
            ){
                // ideas from query compiler by moerkotte: beginning at page 427 with section 24.3 'a first logical profile and its propagation'

                // check if active domains are exclusive
                if(leftMax < rightMin || rightMax < leftMin){
                    // if the active domains are exclusive, then the join selectivity is zero
                    return 0;
                }
                // temp store the maximal join cardinality aka the cross product
                uint64_t maxJoinCardinality = leftCardinality * rightCardinality;
                // determine the join range
                // the minimum in the join range is the maximum of the left and right min
                uint64_t joinMin = std::max(leftMin, rightMin);
                // the maximum in the join range is the minimum of the left and right max
                uint64_t joinMax = std::min(leftMax, rightMax);
                // apply selection with range query predicate
                // if the left relation [leftMin, leftMax] is not completely included in the join range [joinMin, joinMax],
                // then reduce the cardinality of the left relation relatively to the join range by assuming uniform distribution
                if (joinMin != leftMin || joinMax != leftMax) {
                    leftNotNullCount = (static_cast<double>(joinMax - joinMin + 1) / static_cast<double>(leftMax - leftMin + 1))
                         * static_cast<double>(leftNotNullCount);
                }
                // if the right relation [rightMin, rightMax] is not completely included in the join range [joinMin, joinMax],
                // then reduce the cardinality of the right relation relatively to the join range by assuming uniform distribution
                if (joinMin != rightMin || joinMax != rightMax) {
                    rightNotNullCount = (static_cast<double>(joinMax - joinMin + 1) / static_cast<double>(rightMax - rightMin + 1))
                         * static_cast<double>(rightNotNullCount);
                }
                // Query Compile: Page 437 (Profile Propagation for Join: Regular Join)
                // estimate cardinality after join operation according to the reduced left and right join cardinality
                uint64_t joinCardinality = (leftNotNullCount * rightNotNullCount) / (joinMax - joinMin + 1);
                // return estimated join selectivity by dividing the estimated join cardinality with the original maximal join cardinality (cross product)
                return static_cast<double>(joinCardinality) / static_cast<double>(maxJoinCardinality);
            }

            // TODO
            // returns the estimated filter cardinality according to the min value, max value, not null count and cardinality of the column
            static uint64_t estimateFilterCardinality(
                uint64_t min,
                uint64_t max,
                uint64_t cardinality, // notNullCount
                FilterMetaData::Comparison filterPredicate,
                uint64_t constant
            ){
                // if the constant is not in the active domain, then this filter will eliminate all values
                if (constant < min || constant > max) {
                    return 0;
                }
                // calculate the cardinality of the active domain
                double activeDomain = static_cast<double>(max - min + 1);
                // estimated cardinality for one value
                double oneValueCount = static_cast<double>(cardinality) / activeDomain;
                // return the estimated cardinality after the filter predicate
                switch (filterPredicate) {
                    case FilterMetaData::Comparison::Less : {
                        // number of values < constant, i.e., [min, constant): constant - min
                        return static_cast<double>(constant - min)  * oneValueCount;
                    };
                    case FilterMetaData::Comparison::Equal : {
                        return oneValueCount;
                    };
                    case FilterMetaData::Comparison::Greater : {
                        // number of values > constant, i.e., (constant, max]: max - constant
                        return static_cast<double>(max - constant) * oneValueCount;
                    };
                    default : throw std::runtime_error("This filter predicate is not supported.");
                }
            }

        public:
            // create function for regular optimizer inputs, i.e., without given stats
            static void create(
                const database::Database<TABLE_PARTITION_SIZE>& database,
                const std::vector<TableInput>& tablesInput,
                const std::vector<JoinAttributeInput>& innerEquiJoinsInput,
                std::unordered_map<std::string, TableMetaSimplified<TABLE_PARTITION_SIZE>>& targetTablesMap,
                std::vector<std::pair<std::bitset<64>, JoinAttributeMetaSimplified>>& targetInnerEquiJoins
            ){
                // create table meta data including ids, loop over tables and give them a 1-bit-id e.g. "R" -> 0001, "S" -> 0010
                for(uint32_t i=0; i<tablesInput.size(); i++){
                    // retrieve table pointer from database
                    std::shared_ptr<const database::Table<TABLE_PARTITION_SIZE>> permanentTablePointer = database.retrieveTableByNameReadOnly(tablesInput[i]._permanentTableName);
                    // calculate the estimated cardinality for a baste table including filters
                    uint64_t estimatedFilterCardinality = permanentTablePointer->getLatestCardinality();
                    for (const FilterMetaData& filterPredicate : tablesInput[i]._filterPredicates) {
                        // determine the column
                        std::shared_ptr<const database::PermanentColumn<TABLE_PARTITION_SIZE>> columnTyped =
                            permanentTablePointer->retrieveColumnByNameReadOnly(filterPredicate._columnPermanentName);
                        estimatedFilterCardinality = estimateFilterCardinality(
                            columnTyped->getLatestMinValue(),
                            columnTyped->getLatestMaxValue(),
                            estimatedFilterCardinality,
                            filterPredicate._comparison,
                            filterPredicate._constant
                        );
                    }
                    // create and add table meta data to map, key is the alias name of the table
                    auto ret = targetTablesMap.emplace(
                        tablesInput[i]._aliasTableName,
                        TableMetaSimplified(tablesInput[i], permanentTablePointer->getLatestCardinality(), estimatedFilterCardinality,
                            permanentTablePointer, 1ull << i)
                    );
                    if(!ret.second){
                        throw std::runtime_error("Found duplicate table name");
                    }
                }
                // create join bitsets, build inner equi join map e.g. [0011,("R","R_B","S","S_B")]
                for(uint32_t i=0; i<innerEquiJoinsInput.size(); i++){
                    // create the target bitmap that will contain two bits indicating the tables that are joined
                    std::bitset<64> joinBits;
                    // get the bit id of the first table, and determine a pointer to the left column
                    std::shared_ptr<const database::PermanentColumn<TABLE_PARTITION_SIZE>> leftColumn;
                    auto leftTableSearch = targetTablesMap.find( innerEquiJoinsInput[i]._leftTableAliasName);
                    if(leftTableSearch != targetTablesMap.end()) {
                        // set the bit of the table as first join bit
                        joinBits = leftTableSearch->second._optimizerTableId;
                        // determine the left column
                        leftColumn = leftTableSearch->second._permanentTablePointer->retrieveColumnByNameReadOnly(innerEquiJoinsInput[i]._leftColumnPermanentName);
                    }
                    else {
                        throw std::runtime_error("Relation name in join condition not found 1");
                    }
                    // get the bit id of the second table, and determine a pointer to the left column
                    std::shared_ptr<const database::PermanentColumn<TABLE_PARTITION_SIZE>> rightColumn;
                    auto rightTableSearch = targetTablesMap.find( innerEquiJoinsInput[i]._rightTableAliasName);
                    if(rightTableSearch != targetTablesMap.end()) {
                        // also set the bit of the second table
                        joinBits |= rightTableSearch->second._optimizerTableId;
                        // determine the right column
                        rightColumn = rightTableSearch->second._permanentTablePointer->retrieveColumnByNameReadOnly(innerEquiJoinsInput[i]._rightColumnPermanentName);
                    }
                    else {
                        throw std::runtime_error("Relation name in join condition not found 2");
                    }

                    // calculate the selectivity
                    double estimatedSelectivity = estimateJoinSelectivity(
                        leftColumn->getLatestMinValue(),
                        leftColumn->getLatestMaxValue(),
                        leftColumn->getLatestCardinality(),
                        leftTableSearch->second._estimatedCardinalityBaseTable,
                        rightColumn->getLatestMinValue(),
                        rightColumn->getLatestMaxValue(),
                        rightColumn->getLatestCardinality(),
                        rightTableSearch->second._estimatedCardinalityBaseTable
                    );

                    // no join selectivity to estimate, insert the join in the multi map
                    targetInnerEquiJoins.emplace_back(
                        joinBits,
                        JoinAttributeMetaSimplified(
                            innerEquiJoinsInput[i],
                            estimatedSelectivity,
                            joinBits,
                            leftTableSearch->second._optimizerTableId,
                            rightTableSearch->second._optimizerTableId
                        )
                    );
                }
            }
    };

    // standard optimizer and base for 'QueryOptimizerAdaptive' and 'QueryOptimizerOptimalityRanges', supports only the standard search for an optimal plan,
    // contains functions that are used in child classes, works with any of the available template types
    template<
        uint32_t TABLE_PARTITION_SIZE,
        template<uint32_t, template<uint32_t> class,bool> class EnumeratorType,
        template<uint32_t> class PlanClassType,
        bool isPhysicalEnumeration
    >
    class QueryOptimizer {

        protected:
            std::mutex _optimizationMutex;
            std::unordered_map<std::string, TableMetaSimplified<TABLE_PARTITION_SIZE>> _tablesMap; // maps from the table alias name to the table meta data object
            std::vector<std::pair<std::bitset<64>, JoinAttributeMetaSimplified>> _innerEquiJoins;
            EnumeratorType<TABLE_PARTITION_SIZE, PlanClassType, isPhysicalEnumeration> _enumerator;
            std::shared_ptr<FinalNode<TABLE_PARTITION_SIZE>> _optimalPlan = nullptr;

            // invoked at the end of optimization and re-optimization to add a final node on top of the final plan classes optimal plan, sets the '_optimalPlan' member
            void finishOptimization(uint32_t finalPlanClassSize, const std::vector<query_processor::ProjectionMetaData>& projections){
                // choose optimal plan in the final plan class, determine the final plan class' bitset
                std::bitset<64> finalPlanClass;
                for(uint32_t i=0; i<finalPlanClassSize; i++){
                    finalPlanClass.set(i);
                }
                typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator itFinalPlanClass
                    = _enumerator.findPlanClass(finalPlanClass);
                // retrieve the optimal plan of the last plan class and add a default breaker on top
                std::shared_ptr<FinalNode<TABLE_PARTITION_SIZE>> breakerNode( new FinalNode<TABLE_PARTITION_SIZE>(itFinalPlanClass->second.getOptimalPlan(), projections) );
                // set the optimal plan in a member
                _optimalPlan = breakerNode;
            }

        public:
            //
            void optimize(
                const database::Database<TABLE_PARTITION_SIZE>& database,
                const std::vector<TableInput>& tablesInput,
                const std::vector<JoinAttributeInput>& innerEquiJoinsInput,
                const std::vector<query_processor::ProjectionMetaData>& projections
            ){
                // ensure that we do this only once
                std::unique_lock<std::mutex> uLock(_optimizationMutex);
                if(_optimalPlan != nullptr){
                    throw std::runtime_error("Redundant optimize call");
                }

                // print the query
                #ifdef PRINT_QUERY_INPUT
                    std::cout << "Tables:" << std::endl;
                    for(const auto& tableInput : tablesInput){
                        std::cout << " " << tableInput._permanentTableName << "  "<< tableInput._aliasTableName << std::endl;
                    }
                    std::cout << "Joins:" << std::endl;
                    for(const auto& joinInput : innerEquiJoinsInput){
                        std::cout << " " << joinInput._leftTableAliasName << "  "<< joinInput._leftColumnPermanentName << "  " << joinInput._rightTableAliasName
                            << "  "<< joinInput._rightColumnPermanentName << " " << joinInput._isPrimaryKeyForeignKeyJoin << std::endl;
                    }
                    // TODO print projections
                #endif

                // what happens when we optimize only with one table, or only two tables?
                // TODO

                // create table and join meta data including ids
                MetaCreator<TABLE_PARTITION_SIZE>::create(database, tablesInput, innerEquiJoinsInput, _tablesMap, _innerEquiJoins);

                // invoke the enumerator to enumerate all possible plans
                _enumerator.enumerate(_tablesMap, _innerEquiJoins);

                // determine the final plan class, add a final breaker and return
                finishOptimization(_tablesMap.size(), projections);
            }


            // returns the currently optimal plan after an optimization or re-optimization, we do it like this because the
            // adaptive executor only gets a reference to the optimizer object and then retrieves the optimal plan from it
            std::shared_ptr<FinalNode<TABLE_PARTITION_SIZE>> getCurrentOptimalPlan() const {
                if(_optimalPlan == nullptr){
                    throw std::runtime_error("Optimization was not yet invoked");
                }
                return _optimalPlan;
            }


            // returns the optimizer's internal id for a given table
            std::bitset<64> getOptimizerTableId(std::string aliasTableName){
                // look up the 'aliasTableName' in the map and return the id if we found something, otherwise trow an exception
                typename std::unordered_map<std::string, TableMetaSimplified<TABLE_PARTITION_SIZE>>::iterator itTable = _tablesMap.find(aliasTableName);
                if(itTable == _tablesMap.end()){
                    throw std::runtime_error("Table not found");
                }
                return itTable->second._optimizerTableId;
            }


            // prints all the plans in all plan classes in this optimizer
            void printPlanClasses(){
                // determine the final plan class
                std::bitset<64> finalPlanClassId;
                for(uint32_t i=0; i<_tablesMap.size(); i++){
                    finalPlanClassId.set(i);
                }
                // run over the plan classes
                for(uint64_t i = 1; i <= finalPlanClassId.to_ulong(); i++){
                    try{
                        std::bitset<64> planClassBitset(i);
                        typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator it
                            = _enumerator.findPlanClass(planClassBitset);
                        std::cout << "plan class id:\n- " << planClassBitset << std::endl;
                        std::cout << "optimal plan:\n- " << it->second.getOptimalPlan()->toString() << std::endl;
                    }
                    catch(std::exception&){
                    }
                }
            }
    };


    // derived from the standard query optimizer with extensions for re-optimizations, requires adaptive enumerators i.e.
    // having a re-enuermate function, and the adaptive plan class where runtime feedback can be set
    template<
        uint32_t TABLE_PARTITION_SIZE,
        template<uint32_t, template<uint32_t> class,bool> class EnumeratorType,
        template<uint32_t> class PlanClassType,
        bool isPhysicalEnumeration
    >
    class QueryOptimizerAdaptive
    : public QueryOptimizer<TABLE_PARTITION_SIZE, EnumeratorType, PlanClassType, isPhysicalEnumeration>{

        public:
            // does a re-optimization of the query based on runtime feedback for an executed intermediate result/ plan class,
            // finally stores the (new) optimal plan in the '_optimalPlan' member
            void reoptimize(
                std::bitset<64> executedPlanClassId,
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> breaker,
                const std::vector<query_processor::ProjectionMetaData>& projections,
                bool isSelectiveReEnumeration = true
            ){
                // TODO remove
                #ifdef PRINT_REOPTIMIZATION_INFO
                    // start time measuring
                    std::chrono::time_point<std::chrono::system_clock> optimizationStart = std::chrono::system_clock::now();
                #endif

                // ensure that we do only one (re-)optimization at a time
                std::unique_lock<std::mutex> uLock(this->_optimizationMutex);

                // forward the invocation to the enumerator
                if(isSelectiveReEnumeration){
                    // standard case
                    this->_enumerator.reEnumerate(this->_innerEquiJoins, executedPlanClassId, breaker);
                }
                else{
                    // only for heuristic, i.e. we do no re-enumerate at each pipeline breaker
                    this->_enumerator.reEnumerateHeuristic(this->_innerEquiJoins, executedPlanClassId, breaker);
                }

                // determine the final plan class, add a final breaker and return
                this->finishOptimization(this->_tablesMap.size(), projections);

                // TODO remove
                #ifdef PRINT_REOPTIMIZATION_INFO
                    // stop time measuring
                    std::chrono::time_point<std::chrono::system_clock> optimizationEnd = std::chrono::system_clock::now();
                    std::chrono::duration<double> elapsed_seconds = optimizationEnd-optimizationStart;
                    std::cout << " re-optimized plan class: " << executedPlanClassId << std::endl;
                    // std::cout << " runtime cardinality: " << runtimeCardinality << std::endl;
                    std::cout << " new plan: " << this->_optimalPlan->toString() << std::endl;
                    std::cout << " re-optimization time: " << elapsed_seconds.count()*1000000 << "us" << std::endl;
                #endif
            }


            // returns the amount of operators in the query/plan that we optimized, this is invoked in the adaptive executor to make a reotimization decision
            uint32_t getTotalOperatorsCount() const {
                if(this->_optimalPlan == nullptr){
                    throw std::runtime_error("Optimization was not yet invoked");
                }
                // TODO find bulletproof solution for this
                return this->_tablesMap.size()-1;
                // return this->_innerEquiJoins.size(); // this is no option when we have multi column joins
            }

            // this is just used to get infos in the adaptive executor, returns a pointer to the plan class object with the given plan class id
            typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator findPlanClass(std::bitset<64> planClassId){
                // just forward the call to the enumerator
                return this->_enumerator.findPlanClass(planClassId);
            }
    };

}

#endif
