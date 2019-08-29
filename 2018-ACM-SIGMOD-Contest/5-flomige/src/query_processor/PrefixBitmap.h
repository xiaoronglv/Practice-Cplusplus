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

#ifndef PREFIX_BITMAP_H
#define PREFIX_BITMAP_H

#include "../basis/Numa.h"

namespace query_processor {

    class PrefixBitmap{

        // TODO warning for platform dependent code!

        public:
            class PrefixBitmapRow{

                //       MSB                                LSB
                // Bits | 63 |  . . .  | 32 | 31 |  . . .  | 00 |
                //      |<--32 bit prefix-->|<--32 bit Bitmap-->|

                private:
                    uint64_t _value = 0;

                public:
                    bool tryToSetBit(uint16_t bit);

                    uint32_t countSetBits() const;

                    void setPrefix(uint32_t prefixCount);

                    uint32_t calculateIndexBuild(uint16_t bit) const;

                    bool calculateIndexProbe(uint16_t bit, uint32_t& index) const;

                    uint32_t getPrefix() const;

                    std::string print() const;

            };

        private:
            std::vector<PrefixBitmapRow, basis::NumaAllocator<PrefixBitmapRow>> _prefixBitmapRows;
            uint32_t _fillSize=0;
            bool _rowCountSet = false;
            bool _prefixesCalculated = false;

        public:
            PrefixBitmap(uint32_t numaNode);

            void setRowCount(uint64_t rowCount);

            bool tryToSetBit(uint64_t bit);

            void calculatePrefixes();

            uint32_t calculateIndexBuild(uint64_t bit) const;

            bool calculateIndexProbe(uint64_t bit, uint32_t& index) const;

            const PrefixBitmapRow& getPrefixBitmapRow(uint64_t bit) const;

            const PrefixBitmapRow& prefetchPrefixBitmapRow(uint64_t bit) const;

            uint64_t getSize() const;

            uint64_t getFillSize() const;

            std::string print() const;

    };

}

#endif
