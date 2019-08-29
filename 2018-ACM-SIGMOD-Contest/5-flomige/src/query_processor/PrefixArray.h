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

#ifndef PREFIX_ARRAY_H
#define PREFIX_ARRAY_H

#include "../basis/Numa.h"

namespace query_processor {

    template<class CellType>
    class PrefixArray{

        private:
            std::vector<CellType, basis::NumaAllocator<PrefixArray>> _prefixArray;

            CellType _prefixArraySize=0;

            CellType _fillSize=0;
            CellType _calculatedIndex=0;

            bool _prefixesCalculated = false;
            bool _offsetsCalculated = false;

        public:
            PrefixArray(uint32_t numaNode, CellType size)
            : _prefixArray(size, 0, basis::NumaAllocator<PrefixArray>(numaNode)), _prefixArraySize(size) {
            };

            // need for the CHT
            void setRowCount(CellType size) {
                // if(_prefixesCalculated){
                //     throw std::runtime_error("prefixes already calculated for prefix array 0");
                // }
                _prefixArraySize=size;
                _prefixArray.resize(_prefixArraySize, 0);
            }

            void addEntry(CellType index){
                _prefixArray[index]++;
                _fillSize++;
            };

            void calculatePrefixes(){
                // if(_prefixesCalculated){
                //     throw std::runtime_error("prefixes already calculated for prefix array 1");
                // }
                for (CellType i = 1; i < _prefixArraySize; ++i) {
                    _prefixArray[i] += _prefixArray[i-1];
                }
                _prefixesCalculated = true;
            }

            CellType calculateIndexBuild(CellType index) {
                // if(_offsetsCalculated){
                //     throw std::runtime_error("Invoked index calculation in prefix while offsets were already calculated");
                // }
                // if (_prefixArray[index] == 0) {
                //     throw std::runtime_error("index is not valid 1");
                // }
                // subtract the index by one (array starts at zero)
                _prefixArray[index]--;
                // increase the number of tuples calculated the index
                _calculatedIndex++;
                return _prefixArray[index];
            }

            void checkIndexBuild() {
                if (_calculatedIndex == _fillSize) {
                    _offsetsCalculated = true;
                    return;
                }
                throw std::runtime_error("Invoked index calculation in prefix while offsets were not yet build 3; _calculatedIndex: " + std::to_string(_calculatedIndex) + "_fillSize: " + std::to_string(_fillSize));
            }


            bool calculateIndexProbe(CellType index, CellType& start, CellType& end) const { // end is exclusive
                // if(!_offsetsCalculated){
                //     throw std::runtime_error("Invoked index calculation in prefix while offsets were not yet build 3; _calculatedIndex: " + std::to_string(_calculatedIndex) + "_fillSize: " + std::to_string(_fillSize));
                // }
                start = _prefixArray[index];
                if (index + 1 < _prefixArraySize) {
                    // case A: value is not the upper bound
                    end = _prefixArray[index + 1]; // end is exclusive
                } else {
                    // case B: value is the upper bound
                    end = _fillSize; // end is exclusive
                }
                return start != end;
            }

            CellType getFillSize() const {
                return _fillSize;
            }

            CellType getSize() {
                return _prefixArraySize;
            }
    };

}

#endif
