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

#ifndef DATABASE_H
#define DATABASE_H

#include "Table.h"

namespace database{

    template<uint32_t TABLE_PARTITION_SIZE>
    class Database{

        private:
            // each database has a name
            std::string _name;

            // contains tables
            mutable std::shared_mutex _tablesMutex;
            std::unordered_map<std::string, std::shared_ptr<Table<TABLE_PARTITION_SIZE>>> _tablesMap;
            std::vector<std::shared_ptr<Table<TABLE_PARTITION_SIZE>>> _tablesOrdered;

        public:
            // constructor taking the name of the database
            Database(std::string name) {
                // database name upcase representation
                basis::Utilities::validName(name);
                _name=name;
            }

            // returns the name of the database
            const std::string& getName() const {
                return _name;
            }

            // add table to this database that is specified in the table construction description, synchronized, by a mmap file
            std::shared_ptr<Table<TABLE_PARTITION_SIZE>> addTable(TableConstructionDescription<TABLE_PARTITION_SIZE>& descr, uint64_t cardinality, char* addr){
                std::shared_ptr<Table<TABLE_PARTITION_SIZE>> table( new Table<TABLE_PARTITION_SIZE>(descr, cardinality, addr) );
                std::unique_lock<std::shared_mutex> uLock(_tablesMutex);
                auto ret = _tablesMap.emplace(table->getName(), table);
                if(!ret.second){
                    throw std::runtime_error("Table name is already given");
                }
                _tablesOrdered.push_back(table);
                return table;
            }

            // returns a read only pointer to the table in this database that is specified by name, this is mainly used in queries, synchronized
            std::shared_ptr<const Table<TABLE_PARTITION_SIZE>> retrieveTableByNameReadOnly(std::string name) const {
                basis::Utilities::validName(name);
                std::shared_lock<std::shared_mutex> sLock(_tablesMutex);
                auto res = _tablesMap.find(name);
                if(res == _tablesMap.end()){
                    throw std::runtime_error("Table not found 1");
                }
                return res->second;
            }

            // returns a pointer to the table in this database that is specified by name, this is mainly used in write transactions, synchronized
            std::shared_ptr<Table<TABLE_PARTITION_SIZE>> retrieveTableByNameReadWrite(std::string name){
                basis::Utilities::validName(name);
                std::shared_lock<std::shared_mutex> sLock(_tablesMutex);
                auto res = _tablesMap.find(name);
                if(res == _tablesMap.end()){
                    throw std::runtime_error("Table not found 2");
                }
                return res->second;
            }

            // returns the number of tables in this database, synchronized
            std::size_t getTablesSize() const {
                std::shared_lock<std::shared_mutex> sLock(_tablesMutex);
                return _tablesOrdered.size();
            }

            // returns all tables this database is made of
            void getTables(std::vector<Table<TABLE_PARTITION_SIZE>*>& tables) {
                // get a shared lock on this database
                std::shared_lock<std::shared_mutex> sLock(_tablesMutex);
                // run over tables
                for(std::shared_ptr<Table<TABLE_PARTITION_SIZE>>& t : _tablesOrdered){
                    // push table partitions into the target
                    tables.push_back(static_cast<Table<TABLE_PARTITION_SIZE>*>(t.get()));
                }
            }

    };

}

#endif
