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

#ifndef PERMANENT_COLUMN_H
#define PERMANENT_COLUMN_H

#include "../basis/Utilities.h"
#include "../query_processor/TemporaryColumn.h"

#include <limits>

namespace database{

    template<uint32_t TABLE_PARTITION_SIZE>
    class Table;

    template<uint32_t TABLE_PARTITION_SIZE>
    class PermanentColumn : public RawColumn<TABLE_PARTITION_SIZE> {

        private:
            const uint32_t _id;
            std::string _name;
            const Table<TABLE_PARTITION_SIZE>* _table; // raw pointer since columns are created in table constructors, and 'shared_from_this' does not work when there is no shared_pointer yet
            char* _addr;
            uint64_t _minValue = std::numeric_limits<uint64_t>::max()-1; // TODO check for std::numeric_limits<uint64_t>::max()
            uint64_t _maxValue = std::numeric_limits<uint64_t>::min();
            uint64_t _cardinality = 0; // number of entries in this column that is not null, updated under stats mutex
            bool _isDistinct = false;

        public:

            PermanentColumn(
                const uint32_t id,
                const std::string& name,
                const Table<TABLE_PARTITION_SIZE>* table,
                char* addr
            ) : _id(id), _name(name), _table(table), _addr(addr) {
                // check name and make it upper case
                basis::Utilities::validName(_name);
                // ensure that table is not pointing to NULL
                if(_table==nullptr){
                    throw std::invalid_argument("Column points to NULL table");
                }
            }

            ~PermanentColumn(){
            }

            const std::string& getName() const {
                return _name;
            }

            const Table<TABLE_PARTITION_SIZE>* getTable() const {
                return _table;
            }

            uint32_t getId() const {
                return _id;
            }

            void setCardinality(uint64_t cardinality) {
                _cardinality = cardinality;
            }

            void setMinValue(uint64_t value) {
                _minValue = value;
            }

            void setMaxValue(uint64_t value) {
                _maxValue = value;
            }

            void setDistinct(bool isDistinct) {
                _isDistinct = isDistinct;
            }

            uint64_t getLatestMinValue() const {
                return _minValue;
            }

            uint64_t getLatestMaxValue() const {
                return _maxValue;
            }

            uint64_t getLatestCardinality() const {
                return _cardinality;
            }

            bool isDistinct() const {
                return _isDistinct;
            }

            char* getAddr() const {
                return _addr;
            }

            std::shared_ptr<query_processor::TemporaryColumn<TABLE_PARTITION_SIZE>> createTemporaryColumn() const {
                return std::make_shared<query_processor::TemporaryColumn<TABLE_PARTITION_SIZE>>();
            }

    };

}

#endif
