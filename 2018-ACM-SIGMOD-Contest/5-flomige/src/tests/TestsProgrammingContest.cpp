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

#include "QueryInfo.h"
#include "TableLoader.h"
#include "TableCreator.h"
#include "StatisticsBuilder.h"
#include "QueryRunner.h"

#include <limits>
#include <atomic>
#include <iostream>
#include <fstream>

int main() {

    // read table input
    std::vector<std::string> tableIds;
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "Done") {
            break;
        }
        tableIds.push_back(line);
    }

    // load all tables
    TableLoader tableLoader(tableIds);

    // detect which workload
    if (tableLoader.medianTableCardinality() <= 200000) {
        // smaller workload -> partition size = 2048
        // set all parameters
        // sets the size of the bitmap of the cht, i.e., size of the bitmap = CHT_BITMAP_MULTIPLICATOR * getTupleCount()
        query_processor::CHT_BITMAP_MULTIPLICATOR = 32;
        // sets the number of partitions, i.e., number of partitions  = 2 ^ CHT_PARTITION_COUNT_BITS
        query_processor::CHT_PARTITION_COUNT_BITS = 4;
        // sets the number of partitions for CAT
        query_processor::CAT_PARTITION_COUNT = 40;
        // threshold to use CAT by the density of the keys, i.e., range / cardinality
        query_processor::CAT_DENSE_THRESHOLD = 100;
        // threshold to enable the CAT_DENSE_THRESHOLD, i.e., for a small range use CAT, since we have no overhead for hashing
        query_processor::CAT_ENABLE_DENSE_THRESHOLD = 100000;

        // create database
        database::Database<2048> database("DB");

        // create all tables
        TableCreator<2048> tableCreator;
        for (uint32_t t = 0; t < tableIds.size(); ++t) {
            tableCreator.addTableCreatorTask(database, tableLoader._tableIds[t], tableLoader._mmapPointers[t],
                tableLoader._cardinalities[t], tableLoader._columns[t]);
        }
        tableCreator.createTables();

        // Preparation phase (Statistics Building)
        StatisticsBuilder<2048> statisticsBuilder;

        // calculate min and max
        statisticsBuilder.calculateMinAndMax(database, true);

        // detect distinct columns
        statisticsBuilder.calculateDistinct(database, false);

        // read queries
        QueryRunner<2048, query_processor::AdaptiveQueryExecutor<2048>> queryRunner;
        while (std::getline(std::cin, line)) {
            // end of a batch
            if (line == "F") {
                queryRunner.finishBatch();
                continue;
            }
            // add query task to the query runner
            queryRunner.addQuery(database, line);
        }
    }
    else {
        // larger workload -> partition size = 16384
        // set all parameters
        // sets the size of the bitmap of the cht, i.e., size of the bitmap = CHT_BITMAP_MULTIPLICATOR * getTupleCount()
        query_processor::CHT_BITMAP_MULTIPLICATOR = 64;
        // sets the number of partitions, i.e., number of partitions  = 2 ^ CHT_PARTITION_COUNT_BITS
        query_processor::CHT_PARTITION_COUNT_BITS = 4;
        // sets the number of partitions for CAT
        query_processor::CAT_PARTITION_COUNT = 40;
        // threshold to use CAT by the density of the keys, i.e., range / cardinality
        query_processor::CAT_DENSE_THRESHOLD = 100;
        // threshold to enable the CAT_DENSE_THRESHOLD, i.e., for a small range use CAT, since we have no overhead for hashing
        query_processor::CAT_ENABLE_DENSE_THRESHOLD = 100000;

        // create database
        database::Database<16384> database("DB");

        // create all tables
        TableCreator<16384> tableCreator;
        for (uint32_t t = 0; t < tableIds.size(); ++t) {
            tableCreator.addTableCreatorTask(database, tableLoader._tableIds[t], tableLoader._mmapPointers[t],
                tableLoader._cardinalities[t], tableLoader._columns[t]);
        }
        tableCreator.createTables();

        // Preparation phase (Statistics Building)
        StatisticsBuilder<16384> statisticsBuilder;

        // calculate min and max
        statisticsBuilder.calculateMinAndMax(database, true);

        // detect distinct columns
        statisticsBuilder.calculateDistinct(database, false);

        // read queries
        QueryRunner<16384, query_processor::AdaptiveQueryExecutor<16384>> queryRunner;
        while (std::getline(std::cin, line)) {
            // end of a batch
            if (line == "F") {
                queryRunner.finishBatch();
                continue;
            }
            // add query task to the query runner
            queryRunner.addQuery(database, line);
        }
    }
    return 0;
}
