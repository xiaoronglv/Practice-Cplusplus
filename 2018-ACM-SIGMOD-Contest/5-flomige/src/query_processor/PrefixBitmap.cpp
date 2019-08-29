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

#include "PrefixBitmap.h"
#include "../basis/Utilities.h"
#include <string>
namespace query_processor {

    //
    // PrefixBitmap::PrefixBitmapRow
    //

    bool PrefixBitmap::PrefixBitmapRow::tryToSetBit(uint16_t bit){
        // if(bit>31){
        //     throw std::runtime_error("Tried to set a bit that was out of bounds in prefix bitmap row");
        // }
        // set bit and return true if it was not yet set, or return false if it was already set, branch free
        // read bit state
        bool state = (_value >> bit) & 1;
        // set it to 1
        _value |= 1 << bit;
        // return true if it was initially unset, and false otherwise
        return !state;
    }

    uint32_t PrefixBitmap::PrefixBitmapRow::countSetBits() const {
        // run over bits from 0 to 31 and count the 1s, (almost) branch free
        // TODO builtin popcount maybe faster
        uint32_t ones=0;
        for(uint32_t i=0; i<32; i++){
            ones += ((_value >> i) & 1);
        }
        return ones;
    }

    void PrefixBitmap::PrefixBitmapRow::setPrefix(uint32_t prefixCount){ // 32 bit
        // reset prefix count (the 32 most significant bits) to 0
        _value &= 0xffffffff;
        // set prefix count by setting it to a long and shift it to the left
        uint64_t prefixCountLong = prefixCount;
        prefixCountLong = prefixCountLong << 32;
        _value |= prefixCountLong;
    }

    uint32_t PrefixBitmap::PrefixBitmapRow::calculateIndexBuild(uint16_t bit) const {
        // if(bit>31){
        //     throw std::runtime_error("Tried to request a bit that was out of bounds in prefix bitmap row");
        // }
        // // ensure that the bit is set
        // if( !((_value >> bit) & 1)){
        //     throw std::runtime_error("Tried to calculate index of not set bit in PrefixBitmap build");
        // }
        // determine the prefix value
        uint32_t prefix = _value>>32;
        // count the 1s until the bit, (almost) branch free
        uint16_t ones=0;
        for(uint32_t i=0; i<bit; i++){
            ones += ((_value >> i) & 1);
        }
        return prefix + ones;
    }

    bool PrefixBitmap::PrefixBitmapRow::calculateIndexProbe(uint16_t bit, uint32_t& index) const {
        // if(bit>31){
        //     throw std::runtime_error("Tried to request a bit that was out of bounds in prefix bitmap row");
        // }
        // check if the bit is set
        if( !((_value >> bit) & 1)){
            return false;
        }
        // determine the prefix value
        uint32_t prefix = _value>>32;
        // count the 1s until the bit, (almost) branch free
        uint16_t ones=0;
        for(uint32_t i=0; i<bit; i++){
            ones += ((_value >> i) & 1);
        }
        index = prefix + ones;
        return true;
    }

    uint32_t PrefixBitmap::PrefixBitmapRow::getPrefix() const {
        // for tests only
        return _value>>32;
    }

    std::string PrefixBitmap::PrefixBitmapRow::print() const {
        std::string ret = "";
        for(uint32_t i=0; i<32; i++){
            if( ((_value >> i) & 1) ){
                ret += "1";
            }
            else{
                ret += "0";
            }
        }
        ret += "  " + std::to_string(_value>>32) + "\n";
        return ret;
    }


    //
    // PrefixBitmap
    //

    PrefixBitmap::PrefixBitmap(uint32_t numaNode)
    : _prefixBitmapRows(basis::NumaAllocator<PrefixBitmapRow>(numaNode))
    {
        // ensure that uint64_t is 64 bits
        static_assert (sizeof(uint64_t) == 8, "uint64_t is not 64 bit");
    }

    void PrefixBitmap::setRowCount(uint64_t rowCount){
        // rowCount*32 = BitmapSize
        _prefixBitmapRows.resize(rowCount);
        _rowCountSet=true;
    }

    bool PrefixBitmap::tryToSetBit(uint64_t bit){
        // if(!_rowCountSet){
        //     throw std::runtime_error("Prefix bitmap row count not set 1");
        // }
        // if(_prefixesCalculated){
        //     throw std::runtime_error("tryToSetBit prefix calculated 1");
        // }
        // if(_fillSize==std::numeric_limits<uint32_t>::max()){
        //     // maybe overflow table a solution!?
        //     // TODO maybe just return false!?
        //     throw std::runtime_error("bitmap size overflow");
        // }
        if(_prefixBitmapRows[bit/32].tryToSetBit(bit%32)){
            _fillSize++;
            return true;
        }
        return false;
    }

    void PrefixBitmap::calculatePrefixes(){
        // if(!_rowCountSet){
        //     throw std::runtime_error("Prefix bitmap row count not set 2");
        // }
        // if(_prefixesCalculated){
        //     throw std::runtime_error("tryToSetBit prefix calculated 2");
        // }
        uint32_t prefix = 0;
        // run over each prefix bitmap row
        for(PrefixBitmapRow& pbr : _prefixBitmapRows){
            // set the prefix
            pbr.setPrefix(prefix);
            // calculate the local prefix and add it up
            prefix += pbr.countSetBits();
        }
        _prefixesCalculated=true;
    }

    uint32_t PrefixBitmap::calculateIndexBuild(uint64_t bit) const {
        // if(!_prefixesCalculated){
        //     throw std::runtime_error("Invoked index calculation in prefix while prefix was not yet build 1");
        // }
        // determine row and forward call to corresponding row, TODO bitwise AND instead of %
        return _prefixBitmapRows[bit/32].calculateIndexBuild(bit%32);
    }

    bool PrefixBitmap::calculateIndexProbe(uint64_t bit, uint32_t& index) const {
        // if(!_prefixesCalculated){
        //     throw std::runtime_error("Invoked index calculation in prefix while prefix was not yet build 2");
        // }
        // determine row and forward call to corresponding row, TODO bitwise AND instead of %
        return _prefixBitmapRows[bit/32].calculateIndexProbe(bit%32, index);
    }

    const PrefixBitmap::PrefixBitmapRow& PrefixBitmap::getPrefixBitmapRow(uint64_t bit) const{
        return _prefixBitmapRows[bit];
    }

    const PrefixBitmap::PrefixBitmapRow& PrefixBitmap::prefetchPrefixBitmapRow(uint64_t bit) const{
        basis::prefetch_nta(&_prefixBitmapRows[bit]);
        return _prefixBitmapRows[bit];
    }

    uint64_t PrefixBitmap::getSize() const {
        return _prefixBitmapRows.size()*32;
    }

    uint64_t PrefixBitmap::getFillSize() const {
        return _fillSize;
    }

    std::string PrefixBitmap::print() const {
        std::string ret="";
        for(uint32_t i=0; i<_prefixBitmapRows.size(); i++){
            ret += _prefixBitmapRows[i].print();
        }
         ret += "\n_fillSize: " + std::to_string(_fillSize);
        return ret;
    }

}
