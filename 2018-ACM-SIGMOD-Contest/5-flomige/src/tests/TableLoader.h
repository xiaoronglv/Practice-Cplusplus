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

#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <chrono>
#include <algorithm>
#ifdef _WIN32
#include <Windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#endif

class TableLoader {

    public:
        std::vector<std::string> _tableIds;
        std::vector<char*> _mmapPointers;
        std::vector<uint64_t> _cardinalities;
        std::vector<uint64_t> _sortedCardinalities;
        std::vector<uint64_t> _columns;

        TableLoader(std::vector<std::string> tableIds) {
            for (uint32_t t = 0; t < tableIds.size(); ++t) {
#ifdef _WIN32
                HANDLE hFile = ::CreateFile(TEXT(tableIds[t].data()), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
                if (hFile == INVALID_HANDLE_VALUE) {
                    throw std::runtime_error("cannot open " + tableIds[t]);
                }

                LARGE_INTEGER size;
                if (!GetFileSizeEx(hFile, &size))
                {
                    throw std::runtime_error("cannot get the file size of " + tableIds[t]);
                }

                uint64_t length = size.QuadPart;

                HANDLE hMap = ::CreateFileMapping(hFile, NULL, PAGE_READONLY, 0, 0, 0);
                if (hMap == NULL) {
                    throw std::runtime_error("cannot create file mapping for " + tableIds[t]);
                }
                char *addr = (char*)MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, length);
                if (addr == nullptr) {
                    throw std::runtime_error("cannot mmap " + tableIds[t] + " of length " + std::to_string(length));
                }
#else
                // read file
                int fd = open(&tableIds[t][0], O_RDONLY);
                if (fd == -1) {
                    throw std::runtime_error("cannot open " + tableIds[t]);
                }

                // Obtain file size
                struct stat sb;
                if (fstat(fd, &sb) == -1) {
                    throw std::runtime_error("fstat");
                }
                auto length = sb.st_size;

                char* addr=static_cast<char*>(mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, 0u));
                if (addr == MAP_FAILED) {
                    throw std::runtime_error("cannot mmap " + tableIds[t] + " of length " + std::to_string(length));
                }
#endif
                if (length < 16) {
                    throw std::runtime_error("relation file " + tableIds[t] + " does not contain a valid header");
                }

                // store table id
                _tableIds.push_back(tableIds[t]);
                // store the pointer to the mmap file
                _mmapPointers.push_back(addr);

                // store the cardinality number
                uint64_t cardinality = *reinterpret_cast<uint64_t*>(addr);
                addr+=sizeof(uint64_t);
                _cardinalities.push_back(cardinality);
                _sortedCardinalities.push_back(cardinality);

                // store the columns number
                uint64_t numColumns = *reinterpret_cast<uint64_t*>(addr);
                _columns.push_back(numColumns);
#ifdef _WIN32
                // UnmapViewOfFile(addr);
                // CloseHandle(hFile);
#endif
            }
            // sort the cardinalities
            std::sort(_sortedCardinalities.begin(), _sortedCardinalities.end());
        }

        uint64_t medianTableCardinality() {
            return _sortedCardinalities[_sortedCardinalities.size() / 2];
        }
};
