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

#include <limits>
#include <iostream>
#include <fstream>
#include <chrono>

template<uint32_t TABLE_PARTITION_SIZE, class Executor>
class QueryRunner {

    private:
        uint32_t _batchQueryId;
        uint32_t _totalQueryId;
        std::vector<std::vector<std::shared_ptr<basis::TaskBase>>> _tasks;
        std::vector<std::vector<std::string>> _results;

        class QueryTask : public basis::TaskBase {
            private:
                database::Database<TABLE_PARTITION_SIZE>& _database;
                std::string _rawQuery;
                std::vector<std::vector<std::string>>& _results;
                uint32_t _currentQueryId;
                uint32_t _totalQueryId;

            public:
                QueryTask(
                    database::Database<TABLE_PARTITION_SIZE>& database,
                    std::string rawQuery,
                    std::vector<std::vector<std::string>>& results,
                    uint32_t currentQueryId,
                    uint32_t totalQueryId)
                : TaskBase(basis::OLAP_TASK_PRIO,0),
                    _database(database),
                    _rawQuery(rawQuery),
                    _results(results),
                    _currentQueryId(currentQueryId),
                    _totalQueryId(totalQueryId){
                }

                void execute() {
                    // parse query
                    QueryInfo queryInfo(_rawQuery);

                    // optimize and execute the query
                    Executor executor;
                    std::shared_ptr<query_processor::ProjectionBreaker<TABLE_PARTITION_SIZE>> resultBreaker
                         = std::static_pointer_cast<query_processor::ProjectionBreaker<TABLE_PARTITION_SIZE>>(executor.execute(
                            _database, queryInfo.getTables(), queryInfo.getInnerEquiJoins(), queryInfo.getProjections()));

                    // get the checksum of the projection breaker
                    std::vector<uint64_t> checksum;
                    resultBreaker->getChecksum(checksum);
                    // create the string output
                    std::string output;
                    for (uint32_t i = 0; i < checksum.size(); ++i) {
                        if (checksum[i] == 0) {
                            output += "NULL";
                        } else {
                            output += std::to_string(checksum[i]);
                        }
                        if (i+1 < checksum.size()) {
                            output += " ";
                        }
                    }
                    _results.back()[_currentQueryId] = output;
                }
    };


    public:

        QueryRunner()
         : _batchQueryId(0), _totalQueryId(1), _tasks(1), _results(1) {
        }

        void execute() {
            while (true) {

            }
        }

        void addQuery(database::Database<TABLE_PARTITION_SIZE>& database, std::string rawQuery) {
            // append a new result to the result vector
            _results.back().push_back(std::string());
            // create the new query task and add it to the task group
            std::shared_ptr<basis::TaskBase> task(new(_batchQueryId % 2) QueryTask(database, rawQuery, _results, _batchQueryId, _totalQueryId));
            _tasks.back().push_back(task);
            // update the query ids
            ++_batchQueryId;
            ++_totalQueryId;
        };

        void finishBatch() {
            // submit tasks to the task scheduler
            for (std::shared_ptr<basis::TaskBase>& task : _tasks.back()) {
                basis::TaskScheduler::submitTask(task);
            }
            // wait until all query tasks have been finished
            bool allTasksAreFinished = false;
            while (!allTasksAreFinished) {
                allTasksAreFinished = true;
                for (std::shared_ptr<basis::TaskBase>& task : _tasks.back()) {
                    if (!task->isAlreadyFinished()) {
                        allTasksAreFinished = false;
                    }
                }
                // sleep a while if not all tasks have been loaded
                if (allTasksAreFinished) {
                    std::this_thread::sleep_for(std::chrono::microseconds(500));
                }
            }
            // check if all tasks have been executed
            for (std::shared_ptr<basis::TaskBase>& task : _tasks.back()) {
                if(!task->isExecuted()){
                    throw std::runtime_error("Query Task finished with failure: " + task->getFailureMessage());
                }
            }
            // print the results in the right order
            for (uint32_t i = 0; i < _results.back().size(); ++i) {
                std::cout << _results.back()[i] << std::endl;
            }
            // add a new result and task group for the next batch of queries
            _results.push_back(std::vector<std::string>());
            _tasks.push_back(std::vector<std::shared_ptr<basis::TaskBase>>());
            // clear the batch query id counter
            _batchQueryId = 0;
        }
};
