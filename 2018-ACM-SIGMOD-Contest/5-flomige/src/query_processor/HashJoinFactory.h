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

#ifndef HASH_JOIN_FACTORY_H
#define HASH_JOIN_FACTORY_H

// Hash Join Operator CHT for no payload column
#include "HashJoinOperatorCHT.h"
// Hash Join Operator CHT for one payload column
#include "HashJoinOperatorCHTOnePayload.h"
// Hash Join Operator CHT for multi payloads (reference)
#include "HashJoinOperatorCHTMultiPayloads.h"
// Hash Join Operator AT for no payload column
#include "HashJoinOperatorAT.h"
// Hash Join Operator AT for no payload column on a distinct column
#include "HashJoinOperatorATDistinct.h"
// Hash Join Operator CAT for one payload column
#include "HashJoinOperatorCATOnePayload.h"
// Hash Join Operator CAT for one payload column on a distinct column
#include "HashJoinOperatorCATOnePayloadDistinct.h"
// Hash Join Operator CAT for two payload columns
#include "HashJoinOperatorCATTwoPayloads.h"
// Hash Join Operator CAT for multi payloads (reference)
#include "HashJoinOperatorCATMultiPayloads.h"

namespace query_processor {

    // threshold to use CAT by the cardinality of the build side
    static const uint64_t CAT_BUILDSIDE_CARDINALITY_THRESHOLD = 4294967296; // 2^32 = 4294967296
    // threshold to use CAT by the range of the estimated maximum and minimum value
    static const uint64_t CAT_RANGE_THRESHOLD = 268435456; // 2^28 = 268435456

    // threshold to use CAT by the density of the keys, i.e., range / cardinality
    static uint64_t CAT_DENSE_THRESHOLD = 100;
    // threshold to enable the CAT_DENSE_THRESHOLD, i.e., for a small range use CAT, since we have no overhead for hashing
    static uint64_t CAT_ENABLE_DENSE_THRESHOLD = 100000;

    //
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinLateOperatorFactory {

        public:
            static std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> createInstance(
                const std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>>& probeColumnsPointers,
                const std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>>& buildColumnsPointers,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentColumns,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly,
                ColumnIdMappingsContainer& columnIdMappings,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns,
                const std::vector<uint32_t>& probeColumnsPipelineIds,
                std::shared_ptr<DefaultBreaker<TABLE_PARTITION_SIZE>> buildBreaker,
                const std::vector<uint32_t>& buildColumnsPipelineIds,
                RuntimeStatistics* runtimeStatistics
            ){
                // create hash join late operator
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> hashJoin = nullptr;

                // try uint64_t
                if (probeColumnsPointers.size() == 1 && buildColumnsPointers.size() == 1) {
                    uint64_t buildSideCardinality = buildBreaker->getValidRowCount();
                    uint64_t min = buildPipeColumns.getColumns()[buildColumnsPipelineIds.at(0)]._min;
                    uint64_t max = buildPipeColumns.getColumns()[buildColumnsPipelineIds.at(0)]._max;
                    bool isBuildSideDistinct = buildPipeColumns.getColumns()[buildColumnsPipelineIds.at(0)]._isDistinct;
                    uint64_t denseFactor = (max - min + 1) / std::max(buildSideCardinality, std::uint64_t(1u));
                    // check if we use the CAT or the CHT as hash table
                    if (   buildSideCardinality < CAT_BUILDSIDE_CARDINALITY_THRESHOLD // 2^32
                        && !(max - min + 1 > CAT_ENABLE_DENSE_THRESHOLD && denseFactor > CAT_DENSE_THRESHOLD) // for large non dense joins use CHT
                        && max - min + 1 < CAT_RANGE_THRESHOLD // 2^28
                    ) {
                        if (columnIdMappings.getMapping().size() == 0) {
                            // Array Table for join with no payload column
                            if (isBuildSideDistinct) {
                                hashJoin = std::shared_ptr<HashJoinLateOperatorATDistinct<TABLE_PARTITION_SIZE>>(
                                    new HashJoinLateOperatorATDistinct<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                        buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                                );
                            } else {
                                hashJoin = std::shared_ptr<HashJoinLateOperatorAT<TABLE_PARTITION_SIZE>>(
                                    new HashJoinLateOperatorAT<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                        buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                                );
                            }
                        }
                        else if (columnIdMappings.getMapping().size() == 1) {
                            // Concise Array Table Single Value
                            if (isBuildSideDistinct) {
                                hashJoin = std::shared_ptr<HashJoinLateOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE>>(
                                    new HashJoinLateOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                        buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                                );
                            } else {
                                hashJoin = std::shared_ptr<HashJoinLateOperatorCATOnePayload<TABLE_PARTITION_SIZE>>(
                                    new HashJoinLateOperatorCATOnePayload<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                        buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                                );
                            }
                        }
                        else if (columnIdMappings.getMapping().size() == 2) {
                            // Concise Array Table Double Value
                            hashJoin = std::shared_ptr<HashJoinLateOperatorCATTwoPayloads<TABLE_PARTITION_SIZE>>(
                                new HashJoinLateOperatorCATTwoPayloads<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                    buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        }
                        else {
                            // Concise Array Table multi payloads
                            hashJoin = std::shared_ptr<HashJoinLateOperatorCATMultiPayloads<TABLE_PARTITION_SIZE>>(
                                new HashJoinLateOperatorCATMultiPayloads<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                    buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        }
                    }
                    // Concise Hash Table
                    else {
                        if (columnIdMappings.getMapping().size() == 0) {
                            hashJoin = std::shared_ptr<HashJoinLateOperatorCHT<TABLE_PARTITION_SIZE>>(
                                new HashJoinLateOperatorCHT<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                    buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        } else if (columnIdMappings.getMapping().size() == 1) {
                            hashJoin = std::shared_ptr<HashJoinLateOperatorCHTOnePayload<TABLE_PARTITION_SIZE>>(
                                new HashJoinLateOperatorCHTOnePayload<TABLE_PARTITION_SIZE>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                    buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        } else {
                            hashJoin = std::shared_ptr<HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE, uint64_t>>(
                                new HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE, uint64_t>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                                    buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        }
                    }
                    return hashJoin;
                }

                // try uint64_t,uint64_t
                if (probeColumnsPointers.size() == 2 && buildColumnsPointers.size() == 2) {
                    std::shared_ptr<HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t>>(
                        new HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                            buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                    );
                    return hashJoin;
                }

                // try uint64_t,uint64_t,uint64_t
                if (probeColumnsPointers.size() == 3 && buildColumnsPointers.size() == 3) {
                    std::shared_ptr<HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t>>(
                        new HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                            buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                    );
                    return hashJoin;
                }

                // try uint64_t,uint64_t,uint64_t,uint64_t
                if (probeColumnsPointers.size() == 4 && buildColumnsPointers.size() == 4) {
                    std::shared_ptr<HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t,uint64_t>>(
                        new HashJoinLateOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t,uint64_t>(currentColumns, probeSideColumnsOnly, columnIdMappings,
                            buildPipeColumns, probeColumnsPipelineIds, buildBreaker, buildColumnsPipelineIds, runtimeStatistics)
                    );
                    return hashJoin;
                }

                // TODO support other types
                currentColumns.print();
                // throw exception if column type is not supported
                throw std::runtime_error("Hash join is not supported for this column type(s) or column type combination 1");
            }
    };



    //
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinEarlyBreakerFactory {

        public:
            static std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> createInstance(
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentColumns,
                const std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>> buildColumnsPointers,
                const std::vector<uint32_t> buildColumnsPipelineIds,
                std::vector<std::string>& buildColumnsNames,
                uint64_t estimatedBuildSideCardinality,
                RuntimeStatistics* runtimeStatistics
            ){
                // determine the payload column id(s)
                // add columns of build pipeline/build breaker into probe pipeline, this has to take place before the join operator is created
                std::vector<NameColumnStruct<TABLE_PARTITION_SIZE>>& columns = currentColumns.getColumns();
                std::vector<uint32_t> buildBreakerColumnIds;
                for(uint32_t columnId=0; columnId<columns.size(); columnId++){
                    // check if the current column is a join column
                    bool isJoinColumn = false;
                    for (uint32_t i = 0; i < buildColumnsNames.size(); ++i) {
                        // check if the current build column is a join column
                        if (columns[columnId].isName(buildColumnsNames[i])) {
                            isJoinColumn = true;
                            break;
                        }
                    }
                    // if it is wasn't a join column, then we have found a payload column
                    if (!isJoinColumn) {
                        buildBreakerColumnIds.push_back(columnId);
                    }
                }

                // create hash join early breaker
                std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> breaker = nullptr;

                // try uint64_t
                if (buildColumnsPointers.size() == 1) {
                    uint64_t min = currentColumns.getColumns()[buildColumnsPipelineIds.at(0)]._min;
                    uint64_t max = currentColumns.getColumns()[buildColumnsPipelineIds.at(0)]._max;
                    bool isBuildSideDistinct = currentColumns.getColumns()[buildColumnsPipelineIds.at(0)]._isDistinct;
                    uint64_t denseFactor = (max - min + 1) / std::max(estimatedBuildSideCardinality, std::uint64_t(1u));
                    // check if we use the CAT or the CHT as hash table
                    if (   estimatedBuildSideCardinality < CAT_BUILDSIDE_CARDINALITY_THRESHOLD // 2^32
                        && !(max - min + 1 > CAT_ENABLE_DENSE_THRESHOLD && denseFactor > CAT_DENSE_THRESHOLD) // for large non dense joins use CHT
                        && max - min + 1 < CAT_RANGE_THRESHOLD // 2^28
                    ) {
                        if (buildBreakerColumnIds.size() == 0) {
                            if (isBuildSideDistinct) {
                                // Distinct AT with no payload column
                                breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                    new HashJoinEarlyBreakerATDistinct<TABLE_PARTITION_SIZE>(
                                        currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                                );
                            } else {
                                // AT with no payload column
                                breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                    new HashJoinEarlyBreakerAT<TABLE_PARTITION_SIZE>(
                                        currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                                );
                            }

                        }
                        else if (buildBreakerColumnIds.size() == 1) {
                            // CAT as single value
                            if (isBuildSideDistinct) {
                                breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                    new HashJoinEarlyBreakerCATOnePayloadDistinct<TABLE_PARTITION_SIZE>(
                                        currentColumns, buildColumnsNames, buildColumnsPipelineIds, buildBreakerColumnIds[0], runtimeStatistics)
                                );
                            } else {
                                breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                    new HashJoinEarlyBreakerCATOnePayload<TABLE_PARTITION_SIZE>(
                                        currentColumns, buildColumnsNames, buildColumnsPipelineIds, buildBreakerColumnIds[0], runtimeStatistics)
                                );
                            }
                        }
                        else if (buildBreakerColumnIds.size() == 2) {
                            // CAT as double value
                            breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                new HashJoinEarlyBreakerCATTwoPayloads<TABLE_PARTITION_SIZE>(
                                    currentColumns, buildColumnsNames, buildColumnsPipelineIds, buildBreakerColumnIds[0],
                                        buildBreakerColumnIds[1], runtimeStatistics)
                            );
                        }
                        else {
                            // default CAT with multi payload columns
                            breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                new HashJoinEarlyBreakerCATMultiPayloads<TABLE_PARTITION_SIZE>(
                                    currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        }
                    }
                    // Concise Hash Table
                    else {
                        if (buildBreakerColumnIds.size() == 0) {
                            breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                new HashJoinEarlyBreakerCHT<TABLE_PARTITION_SIZE>(
                                    currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        } else if (buildBreakerColumnIds.size() == 1) {
                            breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                new HashJoinEarlyBreakerCHTOnePayload<TABLE_PARTITION_SIZE>(
                                    currentColumns, buildColumnsNames, buildColumnsPipelineIds, buildBreakerColumnIds[0], runtimeStatistics)
                            );
                        } else {
                            breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                                new HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t>(
                                    currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                            );
                        }

                    }
                    return breaker;
                }

                // try uint64_t,uint64_t
                if (buildColumnsPointers.size() == 2) {
                    breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                        new HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t>(
                            currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                    );
                    return breaker;
                }

                // try uint64_t,uint64_t,uint64_t
                if (buildColumnsPointers.size() == 3) {
                    breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                        new HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t>(
                            currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                    );
                    return breaker;
                }

                // try uint64_t,uint64_t,uint64_t,uint64_t
                if (buildColumnsPointers.size() == 4) {
                    breaker = std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>>(
                        new HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t,uint64_t>(
                            currentColumns, buildColumnsNames, buildColumnsPipelineIds, runtimeStatistics)
                    );
                    return breaker;
                }

                // TODO support other types
                currentColumns.print();
                // throw exception if column type is not supported
                throw std::runtime_error("Hash join is not supported for this column type(s) or column type combination 2");
            }
    };



    //
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinEarlyOperatorFactory {

        public:
            static std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> createInstance(
                const std::vector<std::shared_ptr<TemporaryColumn<TABLE_PARTITION_SIZE>>>& probeColumnsPointers,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentColumns,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& probeSideColumnsOnly,
                ColumnIdMappingsContainer& columnIdMappings,
                PipelineColumnsContainer<TABLE_PARTITION_SIZE>& buildPipeColumns,
                const std::vector<uint32_t>& probeColumnsPipelineIds,
                std::shared_ptr<HashJoinEarlyBreakerBase<TABLE_PARTITION_SIZE>> buildBreaker,
                RuntimeStatistics* runtimeStatistics
            ){
                // create hash join early operator
                std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>> hashJoin = nullptr;

                // try uint64_t
                if (probeColumnsPointers.size() == 1) {
                    // check if it is a build breaker of a Distinct Array Table
                    std::shared_ptr<HashJoinEarlyBreakerATDistinct<TABLE_PARTITION_SIZE>> buildBreakerCastedATDistinct
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerATDistinct<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedATDistinct != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorATDistinct<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedATDistinct, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of Array Table
                    std::shared_ptr<HashJoinEarlyBreakerAT<TABLE_PARTITION_SIZE>> buildBreakerCastedAT
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerAT<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedAT != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorAT<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedAT, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Array Table Single Value Distinct
                    std::shared_ptr<HashJoinEarlyBreakerCATOnePayloadDistinct<TABLE_PARTITION_SIZE>> buildBreakerCastedCATOnePayloadDistinct
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCATOnePayloadDistinct<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedCATOnePayloadDistinct != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCATOnePayloadDistinct<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCATOnePayloadDistinct, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Array Table Single Value
                    std::shared_ptr<HashJoinEarlyBreakerCATOnePayload<TABLE_PARTITION_SIZE>> buildBreakerCastedCATOnePayload
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCATOnePayload<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedCATOnePayload != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCATOnePayload<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCATOnePayload, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Array Table Double Value
                    std::shared_ptr<HashJoinEarlyBreakerCATTwoPayloads<TABLE_PARTITION_SIZE>> buildBreakerCastedCATTwoPayloads
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCATTwoPayloads<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedCATTwoPayloads != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCATTwoPayloads<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCATTwoPayloads, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Array Table Multi Payloads
                    std::shared_ptr<HashJoinEarlyBreakerCATMultiPayloads<TABLE_PARTITION_SIZE>> buildBreakerCastedCATMultiPayloads
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCATMultiPayloads<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedCATMultiPayloads != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCATMultiPayloads<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCATMultiPayloads, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Hash Table (with no payloads)
                    std::shared_ptr<HashJoinEarlyBreakerCHT<TABLE_PARTITION_SIZE>> buildBreakerCastedCHT
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCHT<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedCHT != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCHT<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCHT, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Hash Table One Payload
                    std::shared_ptr<HashJoinEarlyBreakerCHTOnePayload<TABLE_PARTITION_SIZE>> buildBreakerCastedCHTOnePayload
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCHTOnePayload<TABLE_PARTITION_SIZE>>(buildBreaker);
                    if(buildBreakerCastedCHTOnePayload != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCHTOnePayload<TABLE_PARTITION_SIZE>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCHTOnePayload, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                    // check if it is a build breaker of a Concise Hash Table Multi Payloads
                    std::shared_ptr<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE, uint64_t>> buildBreakerCastedCHTMultiPayloads
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE, uint64_t>>(buildBreaker);
                    if(buildBreakerCastedCHTMultiPayloads != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE, uint64_t>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCHTMultiPayloads, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                }


                // try uint64_t,uint64_t
                if (probeColumnsPointers.size() == 2) {
                    // check if it is a build breaker of a Concise Hash Table
                    std::shared_ptr<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t>> buildBreakerCastedCHTMultiPayloads
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t>>(buildBreaker);
                    if(buildBreakerCastedCHTMultiPayloads != nullptr){
                        // std::cerr << "CHT2;";
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCHTMultiPayloads, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                }

                // try uint64_t,uint64_t,uint64_t
                if (probeColumnsPointers.size() == 3) {
                    // check if it is a build breaker of a Concise Hash Table
                    std::shared_ptr<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t>> buildBreakerCastedCHTMultiPayloads
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t>>(buildBreaker);
                    if(buildBreakerCastedCHTMultiPayloads != nullptr){
                        // std::cerr << "CHT3;";
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCHTMultiPayloads, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                }

                // try uint64_t,uint64_t,uint64_t,uint64_t
                if (probeColumnsPointers.size() == 4) {
                    // check if it is a build breaker of a Concise Hash Table
                    std::shared_ptr<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t,uint64_t>> buildBreakerCastedCHT
                        = std::dynamic_pointer_cast<HashJoinEarlyBreakerCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t,uint64_t>>(buildBreaker);
                    if(buildBreakerCastedCHT != nullptr){
                        hashJoin = std::shared_ptr<OperatorBase<TABLE_PARTITION_SIZE>>(
                            new HashJoinEarlyOperatorCHTMultiPayloads<TABLE_PARTITION_SIZE,uint64_t,uint64_t,uint64_t,uint64_t>(
                                currentColumns, probeSideColumnsOnly, columnIdMappings,
                                buildPipeColumns, probeColumnsPipelineIds, buildBreakerCastedCHT, runtimeStatistics
                        ));
                        return hashJoin;
                    }
                }

                // TODO support other types
                currentColumns.print();
                // throw exception if column type is not supported
                throw std::runtime_error("Hash join is not supported for this column type(s) or column type combination 3, or there is a mismatch between build column type(s) and probe column type(s) (a wrong build breaker might have been passed)");
            }
    };


}

#endif
