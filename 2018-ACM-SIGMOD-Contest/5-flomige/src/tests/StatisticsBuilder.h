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

#include "../query_processor/QueryExecutor.h"

#include <iostream>
#include <fstream>
#include <random>


template<uint32_t TABLE_PARTITION_SIZE>
class StatisticsBuilder {
    private:

        class MinAndMaxCalculationTask : public basis::TaskBase {

            private:
                database::PermanentColumn<TABLE_PARTITION_SIZE>* _permanentColumn;

            public:
                MinAndMaxCalculationTask(database::PermanentColumn<TABLE_PARTITION_SIZE>* permanentColumn)
                : TaskBase(basis::OLAP_TASK_PRIO,0), _permanentColumn(permanentColumn) {
                }

                void execute() {
                    uint64_t minValue = std::numeric_limits<uint64_t>::max()-1; // TODO check for std::numeric_limits<uint64_t>::max()
                    uint64_t maxValue = std::numeric_limits<uint64_t>::min();
                    uint64_t* currentValue = reinterpret_cast<uint64_t*>(_permanentColumn->getAddr());
                    for (uint32_t i = 0; i < _permanentColumn->getLatestCardinality(); ++i) {
                        if (currentValue[i] < minValue) {
                            minValue = currentValue[i];
                        }
                        if (currentValue[i] > maxValue) {
                            maxValue = currentValue[i];
                        }
                    }
                    _permanentColumn->setMinValue(minValue);
                    _permanentColumn->setMaxValue(maxValue);
                }
        };


        class DistinctCalculationTask : public basis::TaskBase {

            private:
                database::PermanentColumn<TABLE_PARTITION_SIZE>* _permanentColumn;

            public:
                DistinctCalculationTask(database::PermanentColumn<TABLE_PARTITION_SIZE>* permanentColumn)
                : TaskBase(basis::OLAP_TASK_PRIO,0), _permanentColumn(permanentColumn) {
                }

                void execute() {
                    uint64_t minValue = _permanentColumn->getLatestMinValue();
                    uint64_t maxValue = _permanentColumn->getLatestMaxValue();
                    std::vector<bool> bitset(maxValue - minValue + 1, false);
                    uint64_t* currentValue = reinterpret_cast<uint64_t*>(_permanentColumn->getAddr());
                    for (uint32_t i = 0; i < _permanentColumn->getLatestCardinality(); ++i) {
                        if (bitset[currentValue[i] - minValue]) {
                            // duplicate found, i.e., column is not distinct
                            _permanentColumn->setDistinct(false);
                            return;
                        }
                        bitset[currentValue[i] - minValue] = true;
                    }
                    // all values are checked and no duplicate found, i.e., column is distinct
                    _permanentColumn->setDistinct(true);
                }
        };


        std::vector<std::shared_ptr<basis::TaskBase>> _tasks;

    public:

        void calculateMinAndMax(database::Database<TABLE_PARTITION_SIZE>& database, bool wait) {
            // numa distributer
            uint64_t numaDistributionCounter = 0;
            // get the tables from the database
            std::vector<database::Table<TABLE_PARTITION_SIZE>*> tables;
            database.getTables(tables);
            // iterate over all tables
            for (database::Table<TABLE_PARTITION_SIZE>* table : tables) {
                // get the permanent columns
                std::vector<database::PermanentColumn<TABLE_PARTITION_SIZE>*> permanentColumns;
                table->getColumns(permanentColumns);
                // create a min and max calculation task for each column
                for (database::PermanentColumn<TABLE_PARTITION_SIZE>* column : permanentColumns) {
                    // create stats builder task
                    std::shared_ptr<basis::TaskBase> task(new(numaDistributionCounter++%basis::NUMA_NODE_COUNT) MinAndMaxCalculationTask(column));
                    // submit it to the task scheduler
                    basis::TaskScheduler::submitTask(task);
                    // add the task to the task group
                    _tasks.push_back(task);
                }
            }
            // wait until all stats are updated
            if (wait) {
                // wait until all stats have been updated
                bool allStatsAreFinished = false;
                while (!allStatsAreFinished) {
                    allStatsAreFinished = true;
                    for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                        if (!task->isAlreadyFinished()) {
                            allStatsAreFinished = false;
                        }
                    }
                    // sleep a while if not all stats have been updated
                    if (allStatsAreFinished) {
                        std::this_thread::sleep_for(std::chrono::microseconds(500));
                    }
                }
                // check if all tasks have been executed
                for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                    if(!task->isExecuted()){
                        throw std::runtime_error("Stats Task finished with failure: " + task->getFailureMessage());
                    }
                }
            }
        }

        void calculateDistinct(database::Database<TABLE_PARTITION_SIZE>& database, bool wait) {
            // numa distributer
            uint64_t numaDistributionCounter = 0;
            // get the tables from the database
            std::vector<database::Table<TABLE_PARTITION_SIZE>*> tables;
            database.getTables(tables);
            // iterate over all tables
            for (database::Table<TABLE_PARTITION_SIZE>* table : tables) {
                // get the permanent columns
                std::vector<database::PermanentColumn<TABLE_PARTITION_SIZE>*> permanentColumns;
                table->getColumns(permanentColumns);
                // create a min and max calculation task for each column
                for (database::PermanentColumn<TABLE_PARTITION_SIZE>* column : permanentColumns) {
                    // create stats builder task
                    std::shared_ptr<basis::TaskBase> task(new(numaDistributionCounter++%basis::NUMA_NODE_COUNT) DistinctCalculationTask(column));
                    // submit it to the task scheduler
                    basis::TaskScheduler::submitTask(task);
                    // add the task to the task group
                    _tasks.push_back(task);
                }
            }
            // wait until all stats are updated
            if (wait) {
                // wait until all stats have been updated
                bool allStatsAreFinished = false;
                while (!allStatsAreFinished) {
                    allStatsAreFinished = true;
                    for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                        if (!task->isAlreadyFinished()) {
                            allStatsAreFinished = false;
                        }
                    }
                    // sleep a while if not all stats have been updated
                    if (allStatsAreFinished) {
                        std::this_thread::sleep_for(std::chrono::microseconds(500));
                    }
                }
                // check if all tasks have been executed
                for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                    if(!task->isExecuted()){
                        throw std::runtime_error("Stats Task finished with failure: " + task->getFailureMessage());
                    }
                }
            }
        }

};
