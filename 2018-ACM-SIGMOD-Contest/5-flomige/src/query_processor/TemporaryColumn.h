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

#ifndef TEMPORARY_COLUMN_H
#define TEMPORARY_COLUMN_H

#include "../database/Column.h"

#include <map>

namespace query_processor {

    template<uint32_t TABLE_PARTITION_SIZE>
    class TemporaryColumn : public database::RawColumn<TABLE_PARTITION_SIZE> {

        public:
            virtual ~TemporaryColumn(){}

    };

}

#endif
