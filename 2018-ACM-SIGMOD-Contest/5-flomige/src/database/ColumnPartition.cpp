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

#include "ColumnPartition.h"

namespace database {

    template<>
    std::vector<std::mutex> ColumnPartitionPool<2048>::_poolPartitionMutexes(basis::TaskScheduler::getWorkerCount());
    template<>
    std::vector<std::mutex> ColumnPartitionPool<16384>::_poolPartitionMutexes(basis::TaskScheduler::getWorkerCount());

    template<>
    std::atomic<uint32_t> ColumnPartitionPool<2048>::_poolPartitionAccessCounter(0);
    template<>
    std::atomic<uint32_t> ColumnPartitionPool<16384>::_poolPartitionAccessCounter(0);

    template<>
    std::vector<std::forward_list<ColumnPartition<2048>*>> ColumnPartitionPool<2048>::_partitions(basis::TaskScheduler::getWorkerCount());
    template<>
    std::vector<std::forward_list<ColumnPartition<16384>*>> ColumnPartitionPool<16384>::_partitions(basis::TaskScheduler::getWorkerCount());
}
