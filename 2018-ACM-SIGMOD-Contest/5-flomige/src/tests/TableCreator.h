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

#include "../database/Table.h"

template<uint32_t TABLE_PARTITION_SIZE>
class TableCreator {
    private:

        class TableCreatorTask : public basis::TaskBase {

            private:
                database::Database<TABLE_PARTITION_SIZE>& _database;
                std::string& _tableId;
                char* _mmapPointer;
                uint64_t _cardinality;
                uint64_t _numColumns;

            public:
                TableCreatorTask(
                    database::Database<TABLE_PARTITION_SIZE>& database,
                    std::string& tableId,
                    char* mmapPointer,
                    uint64_t cardinality,
                    uint64_t numColumns)
                : TaskBase(basis::OLAP_TASK_PRIO,0),
                    _database(database),
                    _tableId(tableId),
                    _mmapPointer(mmapPointer),
                    _cardinality(cardinality),
                    _numColumns(numColumns) {
                }

                void execute() {
                    // create table
                    database::TableConstructionDescription<TABLE_PARTITION_SIZE> description(_tableId);
                    // skip cardinality and number of columns
                    _mmapPointer+=sizeof(uint64_t);
                    _mmapPointer+=sizeof(uint64_t);
                    for (uint32_t col=0; col < _numColumns; ++col) {
                        description.addColumn(std::to_string(col));
                    }
                    // add the columns to the table
                    _database.addTable(description, _cardinality, _mmapPointer);
                }
        };

        std::vector<std::shared_ptr<basis::TaskBase>> _tasks;

    public:
        // add table task with already mmaped file
        void addTableCreatorTask(
            database::Database<TABLE_PARTITION_SIZE>& database,
            std::string& tableId,
            char* mmapPointer,
            uint64_t cardinality,
            uint64_t numColumns
        ) {
            // create the table task
            std::shared_ptr<basis::TaskBase> task(new(0) TableCreatorTask(database, tableId, mmapPointer, cardinality, numColumns));
            // add the task to the task group
            _tasks.push_back(task);
        }

        bool createTables() {
            // submit tasks to the task scheduler
            for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                basis::TaskScheduler::submitTask(task);
            }
            // wait until all tables have been loaded
            bool allTablesAreCreated = false;
            while (!allTablesAreCreated) {
                allTablesAreCreated = true;
                for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                    if (!task->isAlreadyFinished()) {
                        allTablesAreCreated = false;
                    }
                }
                // sleep a while if not all tables have been loaded
                if (allTablesAreCreated) {
					std::this_thread::sleep_for(std::chrono::microseconds(500));
                }
            }
            // check if all tasks have been executed
            for (std::shared_ptr<basis::TaskBase>& task : _tasks) {
                if(!task->isExecuted()){
                    throw std::runtime_error("Table Load Task finished with failure: " + task->getFailureMessage());
                    return false;
                }
            }
            return true;
        }
};
