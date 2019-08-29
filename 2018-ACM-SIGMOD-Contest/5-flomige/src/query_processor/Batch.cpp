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

#include "Pipeline.h"

namespace query_processor {

    //
    // ColumnIdMappingsContainer
    //

    void ColumnIdMappingsContainer::addRawColumnIdMapping(uint32_t buildBreakerColumnId, uint32_t probePipeColumnId){
        _uInt64Mappings.emplace(buildBreakerColumnId, probePipeColumnId);
    }

    void ColumnIdMappingsContainer::swap(ColumnIdMappingsContainer& other){
        _uInt64Mappings.swap(other._uInt64Mappings);
    }

    const std::map<uint32_t, uint32_t>& ColumnIdMappingsContainer::getMapping() const{
        return _uInt64Mappings;
    }
}
