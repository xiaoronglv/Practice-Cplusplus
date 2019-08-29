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

#ifndef PLAN_CLASS_H
#define PLAN_CLASS_H

#include "ExecutionPlan.h"


namespace query_processor {

    // TableMetaSimplified + JoinAttributeMetaSimplified

    // basically collects different plans that represent the same intermediate result, and keeps the cheapest one
    template<uint32_t TABLE_PARTITION_SIZE>
    class PlanClass {
        protected:
            std::bitset<64> _id;
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> _optimalPlan = nullptr;

        protected:
            // potentially prunes the current optimal plan if the new plan is cheaper, virtual since this is overloaded for Optimiality Ranges
            // TODO check if virtual function call creates a performance issue
            virtual void proposeNewOptimalPlan(std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> plan){
                // in case this is the first plan for this plan class, we set it as optimal plan
                if(_optimalPlan == nullptr){
                    _optimalPlan = plan;
                }
                // in case the new plan's cost are smaller than the current optimal plan's cost, we prune the current optimal plan
                else if(plan->getCost() < _optimalPlan->getCost()){
                    _optimalPlan = plan;
                }
            }

        public:
            // constructor taking the bitset id of the plan class
            PlanClass(std::bitset<64> id)
            : _id(id) {
            }

            virtual ~PlanClass(){
            }

            // invoked in base table plan classes to set a plan containing just the base table
            // specializations for 'TableMetaSimplified+JoinAttributeMetaSimplified' and 'TableMetaProfiles+JoinAttributeMetaProfiles'
            void createNewPlanBaseTable(const TableMetaSimplified<TABLE_PARTITION_SIZE>& tableMeta){
                // create a plan object
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> plan( new BaseTableNode<TABLE_PARTITION_SIZE>(_id, tableMeta) );
                // propose the plan as new optimal plan, base table plans get by definition zero costs
                proposeNewOptimalPlan(plan);
            }

            // invoked to add plans for this plan class that are created out of inner equi join(s)
            // for logical enumeration so this function decides on build and probe side
            // specializations for 'TableMetaSimplified+JoinAttributeMetaSimplified' and 'TableMetaProfiles+JoinAttributeMetaProfiles'
            // TODO add flag to search if there is already an appropriate build breaker, right now we just pick the smaller side as build side
            void createNewPlanHashJoinLogical(
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> leftOptimalPlan,
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> rightOptimalPlan,
                const std::vector<JoinAttributeMetaSimplified>& innerEquiJoinAttributes
            ){
                // calcualte join selectivity,
                double estimatedJoinSelectivity = 1;
                // // just multiply the selectivities of the single join attributes
                // for(const auto& entry : innerEquiJoinAttributes){
                //     estimatedJoinSelectivity *= entry._estimatedSelectivity;
                // }
                // // pick the most selective selctivity
                for(const auto& entry : innerEquiJoinAttributes){
                    if(entry._estimatedSelectivity < estimatedJoinSelectivity){
                        estimatedJoinSelectivity = entry._estimatedSelectivity;
                    }
                }

                // calculate the estimated output cardinality
                uint64_t estimatedOutputCardinality = (uint64_t)(
                    estimatedJoinSelectivity *
                    leftOptimalPlan->getOutputCardinality() *
                    rightOptimalPlan->getOutputCardinality()
                );

                // calculate the cost using c_out as cost function
                // heuristic - build side is smaller side
                uint64_t cost = estimatedOutputCardinality + leftOptimalPlan->getCost() + rightOptimalPlan->getCost();

                // heuristically decide on build and probe side
                bool isLeftSideBuildSide;
                if(leftOptimalPlan->getOutputCardinality() <= rightOptimalPlan->getOutputCardinality()){
                    isLeftSideBuildSide = true;
                }
                else{
                    isLeftSideBuildSide = false;
                }

                // create a plan object
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> plan(
                    new HashJoinNode<TABLE_PARTITION_SIZE>(
                        estimatedOutputCardinality,
                        cost,
                        _id,
                        estimatedJoinSelectivity,
                        leftOptimalPlan,
                        rightOptimalPlan,
                        isLeftSideBuildSide,
                        false,
                        innerEquiJoinAttributes
                    )
                );
                 // propose the plan as new optimal plan
                proposeNewOptimalPlan(plan);
            }


            // invoked to add plans for this plan class that are created out of inner equi join(s)
            // for physical enumeration so this function decides on build and probe side
            // specializations for 'TableMetaSimplified+JoinAttributeMetaSimplified' and 'TableMetaProfiles+JoinAttributeMetaProfiles'
            void createNewPlanHashJoinPhysical(
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> leftOptimalPlan,
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> rightOptimalPlan,
                const std::vector<JoinAttributeMetaSimplified>& innerEquiJoinAttributes,
                bool isLeftSideBuildSide,
                bool ,  // isBuildBreakerAvailable,
                bool // isBuildSideDefaultBreaker
            ){
                // we are not passing a shared pointer to the build breaker here, otherwise the breaker will not be deallocated,
                // we search for the breaker (as shared pointer) at pipeline creation time

                // calcualte join selectivity,
                double estimatedJoinSelectivity = 1;
                // just multiply the selectivities of the single join attributes
                for(const auto& entry : innerEquiJoinAttributes){
                    estimatedJoinSelectivity *= entry._estimatedSelectivity;
                }
                // pick the most selective selctivity
                // for(const auto& entry : innerEquiJoinAttributes){
                //     if(entry._estimatedSelectivity < estimatedJoinSelectivity){
                //         estimatedJoinSelectivity = entry._estimatedSelectivity;
                //     }
                // }

                // calculate the estimated output cardinality
                uint64_t estimatedOutputCardinality = (uint64_t)(
                    estimatedJoinSelectivity *
                    leftOptimalPlan->getOutputCardinality() *
                    rightOptimalPlan->getOutputCardinality()
                );

                // calculate the cost using c_mm, consider cost for building hash table if it is not already existing
                // TODO consider 'isBuildSideDefaultBreaker'
                uint64_t buildSideCardinality = 0;
                // this is a bad idea since it messes up the build side probe side decision
                // if(!isBuildBreakerAvailable){
                    if(isLeftSideBuildSide){
                        buildSideCardinality = leftOptimalPlan->getOutputCardinality();
                    }
                    else{
                        buildSideCardinality = rightOptimalPlan->getOutputCardinality();
                    }
                // }
                uint64_t cost = estimatedOutputCardinality + buildSideCardinality + leftOptimalPlan->getCost() + rightOptimalPlan->getCost();

                // create a plan object
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> plan(
                    new HashJoinNode<TABLE_PARTITION_SIZE>(
                        estimatedOutputCardinality,
                        cost,
                        _id,
                        estimatedJoinSelectivity,
                        leftOptimalPlan,
                        rightOptimalPlan,
                        isLeftSideBuildSide,
                        true,
                        innerEquiJoinAttributes
                    )
                );
                 // propose the plan as new optimal plan
                proposeNewOptimalPlan(plan);

            }

            // returns the optimal plan of this plan class if it already exists
            std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>> getOptimalPlan() const {
                // if(_optimalPlan==nullptr){
                //     throw std::runtime_error("No optimal plan for plan class");
                // }
                return _optimalPlan;
            }

            //
            std::bitset<64> getId() const {
                return _id;
            }
    };



    template<uint32_t TABLE_PARTITION_SIZE>
    class PlanClassAdaptive : public PlanClass<TABLE_PARTITION_SIZE> {

        public:
            //
            PlanClassAdaptive(std::bitset<64> id)
            : PlanClass<TABLE_PARTITION_SIZE>(id){
            }

            // invoked by optimizer to add runtime feedback for this plan class i.e. the exact statistics and the intermediate results in the pipeline breaker
            // TODO taking the runtime cardinality as parameter here is a bit deprecated, the constrictor of the 'IntermediateResultNode' can just invoke
            // 'getTrueCardinality()' on the original plan
            std::shared_ptr<IntermediateResultNode<TABLE_PARTITION_SIZE>> setRuntimeStatistics(std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> breaker){
                // save old optimal plan
                std::shared_ptr<PlanNodeBase<TABLE_PARTITION_SIZE>>& oldOptimalPlan = this->_optimalPlan;
                // ensure that there was not already an intermediate result set
                IntermediateResultNode<TABLE_PARTITION_SIZE>* tempIrNode = dynamic_cast<IntermediateResultNode<TABLE_PARTITION_SIZE>*>(oldOptimalPlan.get());
                if(tempIrNode != nullptr){
                    throw std::runtime_error("Tried to set intermediate result to plan class although there is already an intermediate result");
                }
                // set an 'IntermediateResultNode' as optimal plan in this plan class, as a result each plan created out of this
                // plan class will take cost and cardinality from the 'IntermediateResultNode' with up-to-date runtime statistics
                auto irNode = std::make_shared<IntermediateResultNode<TABLE_PARTITION_SIZE>>(this->_id, breaker, oldOptimalPlan);
                this->_optimalPlan = std::static_pointer_cast<PlanNodeBase<TABLE_PARTITION_SIZE>>(irNode);
                return irNode;
            }

            // invoked during a re-optimization, when we re-optimize with new runtime statistics then we cannot compare the costs of the old optimization
            // with the new costs we get during the reoptimization, so we invoke this function to remove all previously added plans from this plan class
            void clearOptimalPlan(){
                this->_optimalPlan = nullptr;
            }

            // see clearOptimalPlan, this one is used in heuristic re-enumeration which is different to the standard (selective) re-enumeration
            void clearOptimalPlanIfNoIntermediateResult(){
                // see if optimal plan of this plan class is an intermediate result node
                IntermediateResultNode<TABLE_PARTITION_SIZE>* tempIrNode = dynamic_cast< IntermediateResultNode<TABLE_PARTITION_SIZE>*>(this->_optimalPlan.get());
                // set the optimal plan to nullptr if it is no intermediate result node
                if(tempIrNode == nullptr){
                    this->_optimalPlan = nullptr;
                }
            }
    };

}

#endif
