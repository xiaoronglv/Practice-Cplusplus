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

#ifndef HASH_JOIN_EARLY_BREAKER_BASE
#define HASH_JOIN_EARLY_BREAKER_BASE

#include "PipelineBreaker.h"

namespace query_processor {


    //
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinOperatorBase : public FlushingOperator<TABLE_PARTITION_SIZE> {
        public:
            HashJoinOperatorBase(const PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, RuntimeStatistics* runtimeStatistics)
            : FlushingOperator<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics){
            }

            virtual ~HashJoinOperatorBase(){
            }

            virtual void buildHashTable() = 0;

    };


    // this is required as return type in pipeline
    template<uint32_t TABLE_PARTITION_SIZE>
    class HashJoinEarlyBreakerBase : public DefaultBreaker<TABLE_PARTITION_SIZE> {
        protected:
            // after a re-optimization it is necessary to know on which column the hash table was build to potentially reuse it
            std::vector<std::string> _buildColumnsNames;

        public:
            HashJoinEarlyBreakerBase(PipelineColumnsContainer<TABLE_PARTITION_SIZE>& currentPipeColumns, std::vector<std::string>& buildColumnsNames, RuntimeStatistics* runtimeStatistics)
            : DefaultBreaker<TABLE_PARTITION_SIZE>(currentPipeColumns, runtimeStatistics){
                _buildColumnsNames.swap(buildColumnsNames);
            }

            virtual ~HashJoinEarlyBreakerBase(){
            }

            const std::vector<std::string>& getBuildColumnsNames() const {
                return _buildColumnsNames;
            }
     };

}

#endif
