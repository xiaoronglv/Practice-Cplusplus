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

#include "../query_processor/ExecutionPlan.h"
#include "../query_processor/PipelineBreaker.h"

#include <vector>
#include <algorithm>
#include <sstream>
// #include <cassert>

static const std::vector<query_processor::FilterMetaData::Comparison> comparisonTypes {
    query_processor::FilterMetaData::Comparison::Less,
    query_processor::FilterMetaData::Comparison::Greater,
    query_processor::FilterMetaData::Comparison::Equal
};

inline static bool isConstant(std::string& raw) { return raw.find('.') == std::string::npos; }

class QueryInfo {

    private:
        std::vector<query_processor::TableInput> _tables;
        std::vector<query_processor::JoinAttributeInput> _innerEquiJoins;
        std::vector<query_processor::ProjectionMetaData> _projections;

        // parse relation ids <r1> <r2> ...
        void parseRelationIds(std::string& rawRelations) {
            std::vector<std::string> relationIds;
            splitString(rawRelations, relationIds, ' ');
            for (uint32_t aliasId = 0; aliasId < relationIds.size(); ++aliasId) {
                _tables.emplace_back("R" + relationIds[aliasId], std::to_string(aliasId));
            }
        }

        // parse predicate r1.a=r2.b
        void parsePredicate(std::string& rawPredicate) {
            // split predicate
            std::vector<std::string> relCols;
            splitPredicates(rawPredicate, relCols);
            // assert(relCols.size() == 2);
            // assert(!isConstant(relCols[0]) && "left side of a predicate is always a SelectInfo");
            // parse left side
            std::vector<std::string> leftSide;
            splitString(relCols[0], leftSide, '.');
            // check for filter
            if (isConstant(relCols[1])) {
                uint64_t constant = stoul(relCols[1]);
                char compType = rawPredicate[relCols[0].size()];
                // add filter to table
                query_processor::FilterMetaData filter(leftSide[1], constant, query_processor::FilterMetaData::Comparison(compType));
                _tables[std::stoul(leftSide[0])].addFilterPredicate(filter);
                // add used column name to table
                _tables[std::stoul(leftSide[0])].addUsedColumn(leftSide[1]);
            } else {
                // parse right side
                std::vector<std::string> rightSide;
                splitString(relCols[1], rightSide, '.');
                // create join predicate
                query_processor::JoinAttributeInput joinAttribute(leftSide[0], leftSide[1], rightSide[0], rightSide[1]);
                // check if this join predicate was already added
                auto it = std::find(_innerEquiJoins.begin(), _innerEquiJoins.end(), joinAttribute);
                // add new join predicate
                if (it == _innerEquiJoins.end()) {
                  _innerEquiJoins.emplace_back(leftSide[0], leftSide[1], rightSide[0], rightSide[1]);
                  // add used column name to table(s)
                  _tables[std::stoul(leftSide[0])].addUsedColumn(leftSide[1]);
                  _tables[std::stoul(rightSide[0])].addUsedColumn(rightSide[1]);
                }
            }
        }

        // parse predicates r1.a=r2.b&r1.b=r3.c...
        void parsePredicates(std::string& text) {
          std::vector<std::string> predicateStrings;
          splitString(text, predicateStrings, '&');
          for (auto& rawPredicate : predicateStrings) {
            parsePredicate(rawPredicate);
          }
        }

        // parse selections r1.a r1.b r3.c...
        void parseProjections(std::string& rawProjections) {
          std::vector<std::string> projectionStrings;
          splitString(rawProjections, projectionStrings, ' ');
          for (auto& rawSelect : projectionStrings) {
            std::vector<std::string> projection;
            splitString(rawSelect, projection, '.');
            // assert(projection.size() == 2);
            _projections.emplace_back(projection[0], projection[1]);
            // add used column name to table
            _tables[std::stoul(projection[0])].addUsedColumn(projection[1], true); // boolean indicates that it is a projection column
          }
        }
        // parse selections [RELATIONS]|[PREDICATES]|[SELECTS]
        void parseQuery(std::string& rawQuery) {
            std::vector<std::string> queryParts;
            splitString(rawQuery, queryParts, '|');
            // assert(queryParts.size()==3);
            parseRelationIds(queryParts[0]);
            parsePredicates(queryParts[1]);
            parseProjections(queryParts[2]);
        }

    public:
        // the constructor that parses a query
        QueryInfo(std::string rawQuery) {
            parseQuery(rawQuery);
        }


        // parse a line into strings
        static void splitString(std::string& line, std::vector<std::string>& result, const char delimiter) {
          std::stringstream ss(line);
          std::string token;
          while (std::getline(ss, token, delimiter)) {
            result.push_back(token);
          }
        }

        // split a line into predicate strings
        static void splitPredicates(std::string& line, std::vector<std::string>& result) {
          for (auto cT : comparisonTypes) {
            if (line.find(cT) != std::string::npos) {
              splitString(line, result, cT);
              break;
            }
          }
        }

        // returns the table meta data
        std::vector<query_processor::TableInput>& getTables() {
            return _tables;
        }

        // returns the join attribute meta data
        std::vector<query_processor::JoinAttributeInput>& getInnerEquiJoins() {
            return _innerEquiJoins;
        }
        // returns the projection meta data
        std::vector<query_processor::ProjectionMetaData>& getProjections() {
            return _projections;
        }

};
