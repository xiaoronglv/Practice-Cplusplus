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

#ifndef COLUMN_H
#define COLUMN_H

#include "TablePartition.h"

#include <memory>

namespace database{

    // some operators e.g. equi joins just care about the raw data type that is stored in the column
    // so 'EncodedColumn's always store uint64_ts and 'UnencodedColumn's the raw type
    template<uint32_t TABLE_PARTITION_SIZE>
    class RawColumn {
        public:
            // implementation of interface function
            ColumnPartition<TABLE_PARTITION_SIZE>* createColumnPartition(uint32_t numaNode) const {
                // create a new column partition
                return new(numaNode) ColumnPartition<TABLE_PARTITION_SIZE>();
            }

            // implementation of interface function
            ColumnPartition<TABLE_PARTITION_SIZE>* createColumnPartition(char* addr) const {
                // create a new column partition by reinterpret_cast
                return reinterpret_cast<ColumnPartition<TABLE_PARTITION_SIZE>*>(addr - sizeof(uint64_t));
            }

            // implementation of interface function
            ColumnPartition<TABLE_PARTITION_SIZE>* createColumnPartitionTryPool(uint32_t numaNode) const {
                // try to get a column partition from the pool
                ColumnPartition<TABLE_PARTITION_SIZE>* partition = ColumnPartitionPool<TABLE_PARTITION_SIZE>::tryPopColumnPartition();
                if(partition != nullptr){
                    return partition;
                }
                // create a new column partition
                return new(numaNode) ColumnPartition<TABLE_PARTITION_SIZE>();
            }
    };

}

#endif
