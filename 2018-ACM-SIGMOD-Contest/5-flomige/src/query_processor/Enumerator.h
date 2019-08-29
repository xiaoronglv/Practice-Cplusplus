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

#ifndef ENUMERATOR_H
#define ENUMERATOR_H

#include "PlanClass.h"

#include <unordered_set>

namespace query_processor {

    // base class for enumerators, containing data structures and functions that every enumerator needs
    template<uint32_t TABLE_PARTITION_SIZE, template<uint32_t> class PlanClassType, bool isPhysicalEnumeration>
    class EnumeratorBase {

        protected:
            std::unordered_map<std::bitset<64>, PlanClassType<TABLE_PARTITION_SIZE>> _planClasses;


            // returns true when 'leftPlanClass' and 'rightPlanClass' can be joined by one or multiple join attributes, and stores all
            // the join attributes in 'target', invoked during optimization and re-optimization
            bool findJoins(
                std::bitset<64> leftPlanClass,
                std::bitset<64> rightPlanClass,
                const std::vector<std::pair<std::bitset<64>, JoinAttributeMetaSimplified>>& innerEquiJoins,
                std::vector<JoinAttributeMetaSimplified>& target
            ){
                // make sure the 'target' vector is empty
                target.clear();
                // check that left and right plan do not overlap
                if((leftPlanClass & rightPlanClass).count() == 0){
                    // check that there is a join, run over the join conditions ('innerEquiJoins'),
                    // and check whether there is a join condition that joins both plan classes
                    for(auto itJoin = innerEquiJoins.begin(); itJoin != innerEquiJoins.end(); itJoin++){
                        // check that the intersection of each sub plan with the join has exactly one bit
                        if((leftPlanClass & itJoin->first).count() == 1 && (rightPlanClass & itJoin->first).count() == 1){
                            // copy the join meta data into the target vector
                            target.push_back(itJoin->second);
                            // now we check if the left and the right side in the copied meta is correct, so if there is a match
                            // between the 'leftPlanClass' id and the '_rightTableId' in the copied meta, we have to switch sides
                            if( (leftPlanClass & target.back()._rightTableId).any() ){
                                target.back().switchLeftAndRightSide();
                            }
                        }
                    }
                }
                return !target.empty();
            }


            // invoked at enumeration during optimization and re-optimization to propose a new plan to a plan class
            // overloaded in adaptive enumerator
            template<bool isReEnum>
            void proposeJoinPlanToPlanClass(
                PlanClassType<TABLE_PARTITION_SIZE>& planClass,
                std::bitset<64> leftPlanClass,
                std::bitset<64> rightPlanClass,
                const std::vector<JoinAttributeMetaSimplified>& innerEquiJoinAttributes
            ){
                // TODO remove
                // std::cout << std::bitset<64>(leftPlanClass) << std::endl;
                // std::cout << std::bitset<64>(rightPlanClass) << std::endl;
                // std::cout << std::bitset<64>(itJoin->first) << std::endl;
                // std::cout << std::bitset<64>(planClass) << std::endl << std::endl;

                // determine pointer to left sub plan class
                auto itLeftPlanClass = _planClasses.find(leftPlanClass);
                // if(itLeftPlanClass == _planClasses.end()){
                //     throw std::runtime_error("Left plan class not found");
                // }
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> leftOptimalPlan = itLeftPlanClass->second.getOptimalPlan();
                // determine pointer to left sub plan class
                auto itRightPlanClass = _planClasses.find(rightPlanClass);
                // if(itRightPlanClass == _planClasses.end()){
                //     throw std::runtime_error("Right plan class not found");
                // }
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> rightOptimalPlan = itRightPlanClass->second.getOptimalPlan();

                // different implementations for logical and physical enumeration
                if(!isPhysicalEnumeration){
                    // propose just a logical plan, the plan class will decide about build and probe side and what can potentially be reused
                    planClass.createNewPlanHashJoinLogical(
                        leftOptimalPlan,
                        rightOptimalPlan,
                        innerEquiJoinAttributes
                    );
                }
                // we are not passing a shared pointer to the build breaker here, otherwise the breaker will not be deallocated,
                // we search for the breaker (as shared pointer) at pipeline creation time
                else{
                    // propose the first plan with left plan as build side
                    planClass.createNewPlanHashJoinPhysical(
                        leftOptimalPlan,
                        rightOptimalPlan,
                        innerEquiJoinAttributes,
                        true,
                        false,
                        false
                    );
                    // propose the second plan with right plan as build side
                    planClass.createNewPlanHashJoinPhysical(
                        leftOptimalPlan,
                        rightOptimalPlan,
                        innerEquiJoinAttributes,
                        false,
                        false,
                        false
                    );
                }

            }


        public:
            virtual ~EnumeratorBase(){
            }

            // returns a pointer to the plan class object with the given plan class id
            typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator findPlanClass(std::bitset<64> planClassId){
                typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator itPlanClass = _planClasses.find(planClassId);
                // if(itPlanClass == _planClasses.end()){
                //     throw std::runtime_error("Plan class not found");
                // }
                return itPlanClass;
            }


            // returns a reference to the plan classes map, this is used in range calculation to access the plan classes
            // TODO we do not return a const reference here :(
            std::unordered_map<std::bitset<64>, PlanClassType<TABLE_PARTITION_SIZE>>& getPlanClasses(){
                return _planClasses;
            }
    };



    // standard DPsize dynamic programming join enumerator that enumerate all possible plans for the given table and join inputs,
    // this is also the base class for an adaptive version
    template<uint32_t TABLE_PARTITION_SIZE, template<uint32_t> class PlanClassType, bool isPhysicalEnumeration>
    class DPSizeEnumerator : public EnumeratorBase<TABLE_PARTITION_SIZE,PlanClassType,isPhysicalEnumeration> {

        protected:
            std::vector<std::vector<std::bitset<64>>> _planClassesBySize;

            // checks if two plan classes can be joined, determines the resulting plan class and proposes the newplan to the plan class
            void checkAndProposeNewPlan(
                std::bitset<64> leftPlanClass,
                std::bitset<64> rightPlanClass,
                const std::vector<std::pair<std::bitset<64>, JoinAttributeMetaSimplified>>& innerEquiJoins
            ){
                // check if we can join 'leftPlanClass' with 'rightPlanClass' and retrieve the join attribute(s), 'foundInnerEquiJoins' is cleared in 'findJoins'
                std::vector<JoinAttributeMetaSimplified> foundInnerEquiJoins;
                if(this->findJoins(leftPlanClass, rightPlanClass, innerEquiJoins, foundInnerEquiJoins)){
                    // determine plan class
                    std::bitset<64> planClass = leftPlanClass | rightPlanClass;
                    // search for plan class
                    typename std::unordered_map<std::bitset<64>, PlanClassType<TABLE_PARTITION_SIZE>>::iterator itPlanClass =
                        this->_planClasses.find(planClass);
                    // if plan class was not yet created
                    if(itPlanClass == this->_planClasses.end()){
                        // add the plan class to the main plan class map
                        auto ret = this->_planClasses.emplace(planClass, PlanClassType<TABLE_PARTITION_SIZE>(planClass));
                        // if(!ret.second){
                        //     throw std::runtime_error("Tried to add plan class again. This should never happen.");
                        // }
                        // update the plan class pointer
                        itPlanClass = ret.first;
                        // add the plan class to '_planClassesBySize'
                        _planClassesBySize[planClass.count()-1].push_back(planClass);
                    }
                    // propose the new plan to the plan class
                    this->template proposeJoinPlanToPlanClass<false>(itPlanClass->second, leftPlanClass, rightPlanClass, foundInnerEquiJoins);
                }
            }


            // invoked by 'enumerate' and 'reEnumerateHeuristic' to enumerate all plans beyond the base table level, could be that the plan
            // classes do not exist (enumerate), have no optimal plan anymore or already contain intermediate results (reEnumerateHeuristic)
            void enumerateNonBaseTablePlanClasses(
                const std::vector<std::pair<std::bitset<64>, JoinAttributeMetaSimplified>>& innerEquiJoins,
                uint32_t finalPlanClassSize
            ){
                // loop over the plan class sizes starting from '2'
                for(uint32_t planClassSize=2; planClassSize<=finalPlanClassSize; ++planClassSize){
                    // add a new entry in '_planClassesBySize' if necessary
                    if(_planClassesBySize.size() < planClassSize){
                        _planClassesBySize.emplace_back();
                    }
                    // create different sub plan size e.g. 7 -> 1+6, 2+5 and 3+4
                    for(uint32_t outerPlanClassSize=1; outerPlanClassSize<=planClassSize/2; ++outerPlanClassSize){
                        // calculate inner plan class size
                        uint32_t innerPlanClassSize = planClassSize - outerPlanClassSize;
                        // if both sub plan classes have the same size, we have to ensure that we do not enumerate plans twice
                        if(outerPlanClassSize == innerPlanClassSize){
                            // try to combime each outer plan with each inner plan, loop over all plans of 'outerPlanClassSize'
                            for(uint32_t i=0; i < _planClassesBySize[outerPlanClassSize-1].size(); ++i){
                                for(uint32_t j=i+1; j<_planClassesBySize[innerPlanClassSize-1].size(); ++j){
                                    checkAndProposeNewPlan(
                                        _planClassesBySize[outerPlanClassSize-1][i],
                                        _planClassesBySize[innerPlanClassSize-1][j],
                                        innerEquiJoins
                                    );
                                }
                            }
                        }
                        // if the sub plans have different sizes
                        else{
                            // try to combime each outer plan with each inner plan, loop over all plans of 'outerPlanClassSize'
                            for(std::bitset<64> outerPlanClass : _planClassesBySize[outerPlanClassSize-1]){
                                // loop over all plans of 'innerPlanClassSize'
                                for(std::bitset<64> innerPlanClass : _planClassesBySize[innerPlanClassSize-1]){
                                    checkAndProposeNewPlan(outerPlanClass, innerPlanClass, innerEquiJoins);
                                }
                            }
                        }
                    }
                }
            }

        public:
            // enumerates all the plans and proposes them to 'checkAndProposeNewPlan'
            // TODO inputs as const &
            void enumerate(
                std::unordered_map<std::string, TableMetaSimplified<TABLE_PARTITION_SIZE>>& tablesMap,
                const std::vector<std::pair<std::bitset<64>, JoinAttributeMetaSimplified>>& innerEquiJoins
            ){
                // enumerating the plans, start with plan classes/plans of size '1' (there is only one plan for plan classes of size '1')
                _planClassesBySize.emplace_back();
                // for each base table
                for(typename std::unordered_map<std::string, TableMetaSimplified<TABLE_PARTITION_SIZE>>::const_iterator itTables = tablesMap.begin(); itTables != tablesMap.end(); itTables++){
                    // create a plan class and add it to '_planClasses'
                    auto ret = this->_planClasses.emplace(itTables->second._optimizerTableId, PlanClassType<TABLE_PARTITION_SIZE>(itTables->second._optimizerTableId));
                    // if(!ret.second){
                    //     throw std::runtime_error("Enumerated plan class again");
                    // }
                    // add the plan class to '_planClassesBySize'
                    _planClassesBySize[0].push_back(itTables->second._optimizerTableId);
                    // add the trivial plan to the plan class
                    ret.first->second.createNewPlanBaseTable(itTables->second);
                }

                // enumerate non base table plan classes, this is a function to avoid duplicate code in derived classes
                enumerateNonBaseTablePlanClasses(innerEquiJoins, tablesMap.size());
            }
    };



    // standard DPSize enumerator with extensions for re-enumeration, i.e., adaptive execution
    template<uint32_t TABLE_PARTITION_SIZE, template<uint32_t> class PlanClassType, bool isPhysicalEnumeration>
    class DPSizeEnumeratorAdaptive : public DPSizeEnumerator<TABLE_PARTITION_SIZE,PlanClassType,isPhysicalEnumeration> {

        private:
            // we want to reuse intermediate results, in an adaptive execution this is always invoked when we are in the re-optimization window, eihter by 'reoptimize'
            // when the estimation was wrong, it basically removes all obsolete plan classes from the '_planClassesBySize' member
            void cleanPlanClassesBySize(std::bitset<64> executedPlanClassId){
                // clean '_planClassesBySize', remove all plan classes that cannot be used anymore, e.g. when we have a bushy tree R-S-T-U-V
                // and have a result for RS and then re-optimize for UV then we don't want plans like R-STUV be considered again, so we remove
                // all plans from '_planClassesBySize' that do not fulfill the below criteria
                for(std::vector<std::bitset<64>>& planClasses : this->_planClassesBySize){
                    // create a new temporary plan class vector
                    std::vector<std::bitset<64>> cleanedPlanClasses;
                    cleanedPlanClasses.reserve(planClasses.size());
                    // run over all plan classes in 'planClasses'
                    for(std::bitset<64>& planClassId : planClasses){
                        // when 'planClassId' and 'executedPlanClassId' are disjunct (intersection is empty) or 'executedPlanClassId' is a subset of 'planClassId'
                        if( (planClassId & executedPlanClassId).none() || ((planClassId & executedPlanClassId) == executedPlanClassId) ){
                            // copy 'planClassId' to 'cleanedPlanClasses'
                            cleanedPlanClasses.push_back(planClassId);
                        }
                    }
                    // swap 'planClasses' with 'cleanedPlanClasses'
                    planClasses.swap(cleanedPlanClasses);
                }
            }


        public:

            // re-enumerates the plan class starting from a given plan class id using up to date runtime statistics, basically updates the
            // plan classes by size data structure by removing obsolete plan classes, updates the statistics and costs for the executed plan
            // class, and re-enumerates only the plan classes that were affected by the cost and cardinality update -> selective re-enumeration
            void reEnumerate(
                const std::vector<std::pair<std::bitset<64>,JoinAttributeMetaSimplified>>& innerEquiJoins,
                std::bitset<64> executedPlanClassId,
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> breaker
            ){
                // clean '_planClassesBySize', remove all plan classes that that became obsolete by plan class being executed (executedPlanClassId)
                cleanPlanClassesBySize(executedPlanClassId);

                // update the plan class for which we already have an executed result, also clear the plan class or if 'clearOptimalPlan()' has to be invoked first
                typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator itExecutedPlanClass
                    = this->findPlanClass(executedPlanClassId);
                itExecutedPlanClass->second.setRuntimeStatistics(breaker);

                // will only contain ids of plan classes that were re-enumerated i.e. contain the 'executedPlanClassId'
                // this is mainly neccessary to figure out if a plan class was already touched by this enumeration
                std::vector<std::unordered_set<std::bitset<64>>> localPlanClassesBySize;
                // also represents an offset to avoid that the first sets in 'localPlanClassesBySize' stay empty
                uint32_t executedPlanClassSize = executedPlanClassId.count();

                // add the 'executedPlanClassId' as only entry in the first set of 'localPlanClassesBySize'
                localPlanClassesBySize.emplace_back();
                localPlanClassesBySize[0].emplace(executedPlanClassId);

                // re-enumerate all relevant plans using modified dp_size, statring with plans of the size 'executedPlanClassSize+1'
                for(uint32_t planClassSize = executedPlanClassSize+1;
                    planClassSize <= this->_planClassesBySize.size();
                    ++planClassSize
                ){
                    // add a new entry in 'localPlanClassesBySize' for this plan class size
                    localPlanClassesBySize.emplace_back();

                    // re-enumerate plans containing the 'executedPlanClassId', the outer plan classes are the ones in 'localPlanClassesBySize',
                    // inner plan classes will be the ones in '_planClassesBySize', outerSize + innerSize = planClassSize,
                    // e.g. executedPlanClassSize = 2 then: 3 = 2+1;  4 = 2+2 = 3+1;  5 = 2+3 = 3+2 = 4+1;  6 = 2+4 = 3+3 = 4+2 = 5+1; ...
                    std::vector<JoinAttributeMetaSimplified> foundInnerEquiJoins;
                    for(uint32_t outerPlanClassSize=executedPlanClassSize; outerPlanClassSize<planClassSize; ++outerPlanClassSize){
                        // calculate inner plan class size
                        uint32_t innerPlanClassSize = planClassSize - outerPlanClassSize;
                        // try to combime each outer plan with each inner plan, loop over all plans of 'outerPlanClassSize'
                        for(std::bitset<64> outerPlanClass : localPlanClassesBySize[outerPlanClassSize-executedPlanClassSize]){
                            // loop over all plans of 'innerPlanClassSize'
                            for(std::bitset<64> innerPlanClass : this->_planClassesBySize[innerPlanClassSize-1] ){
                                // from here on it is basically similiar to 'checkAndProposeNewPlan' but there is no dedicated function since we do it only once
                                // check if we can join 'outerPlanClass' with 'innerPlanClass' and retrieve the join attribute(s), 'foundInnerEquiJoins' is cleared in 'findJoins'
                                if(this->findJoins(outerPlanClass, innerPlanClass, innerEquiJoins, foundInnerEquiJoins)){
                                    // determine plan class
                                    std::bitset<64> planClass = outerPlanClass | innerPlanClass;
                                    // search for plan class in the '_planClasses' member
                                    typename std::unordered_map<std::bitset<64>, PlanClassType<TABLE_PARTITION_SIZE>>::iterator itPlanClass
                                        = this->findPlanClass(planClass);
                                    // try to insert plan class id in the 'localPlanClassesBySize'
                                    auto ret = localPlanClassesBySize.back().insert(planClass);
                                    // clear the plan class object when its id was succesfully inserted into 'localPlanClassesBySize',
                                    // i.e. the plan class was not touched by this re-optimization yet
                                    if(ret.second){
                                        itPlanClass->second.clearOptimalPlan();
                                    }
                                    // propose the new plan to the plan class
                                    this->template proposeJoinPlanToPlanClass<true>(itPlanClass->second, outerPlanClass, innerPlanClass, foundInnerEquiJoins);
                                }
                            }
                        }
                    }
                }
            }


            //
            void reEnumerateHeuristic(
                const std::vector<std::pair<std::bitset<64>,JoinAttributeMetaSimplified>>& innerEquiJoins,
                std::bitset<64> executedPlanClassId,
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> breaker
            ){
                // clean '_planClassesBySize', remove all plan classes that that became obsolete by plan class being executed (executedPlanClassId)
                cleanPlanClassesBySize(executedPlanClassId);

                // update the plan class for which we already have an executed result, also clear the plan class or if 'clearOptimalPlan()' has to be invoked first
                typename std::unordered_map<std::bitset<64>,PlanClassType<TABLE_PARTITION_SIZE>>::iterator itExecutedPlanClass
                    = this->findPlanClass(executedPlanClassId);
                itExecutedPlanClass->second.setRuntimeStatistics(breaker);

                // clear all plan classes that are no base table plan classes, and where the optimal plan is no intermediate result
                for(auto& planClassEntry : this->_planClasses){
                    // no base table
                    if(planClassEntry.first.count() > 1){
                        planClassEntry.second.clearOptimalPlanIfNoIntermediateResult();
                    }
                }

                // re-enumerate all existing plan classes starting from plan class size '2',
                // '_planClassesBySize.size()' indicates the final plan class size
                this->enumerateNonBaseTablePlanClasses(innerEquiJoins, this->_planClassesBySize.size());
            }
    };

}

#endif
