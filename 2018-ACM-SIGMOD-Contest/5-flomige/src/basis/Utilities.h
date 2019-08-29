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

#ifndef UTILITIES_H
#define UTILITIES_H

#include <cstddef> // nullptr_t
#include <iomanip>
#include <locale>
#include <vector>
#include <sstream>
#include <immintrin.h>

namespace basis{

    inline void prefetch_nta(const void* it) noexcept {
    	_mm_prefetch((const char*)it, _MM_HINT_NTA);
    }

    class Utilities {

        public:
            // checks that the name in not empty and contains only graphical characters, furthermore it makes the name uppercase
            static void validName(std::string& name){
                if(name.empty()){
                    throw std::domain_error("Name is empty");
                }
                std::locale loc;
                for(char& c : name){
                    if(!isgraph(c)){
                        throw std::domain_error("Name '" + name + "' contains non graphical character");
                    }
                    c=std::toupper(c,loc);
                }
            }

            // ceiling function of an uint32_t without casting. this is used by the statistics builder
            static uint32_t uint32_ceil(uint32_t a, uint32_t b) {
                return ((a % b) ? a / b + 1 : a / b);
            }

            // ceiling function of an uint64_t without casting. this is used by the table constructor of a mmap file
            static uint64_t uint64_ceil(uint64_t a, uint64_t b) {
                return ((a % b) ? a / b + 1 : a / b);
            }

            // function to parse a string to a latex compatible text string
            static std::string replaceUnderscoreLatex(const std::string& str) {
                std::string output("");
                for(const char& c : str) {
                    switch(c) {
                        // '_' is used for underscore in latex, replace by a '\textunderscore'
                        case '_': output += "\\textunderscore "; break;
                        default: output += c; break;
                    }
                }
                return output;
            }

            // adds commas to large numbers
            template<class T>
            static std::string formatWithCommas(T value) {
                std::stringstream ss;
                ss.imbue(std::locale(""));
                ss << std::fixed << std::setprecision(2) << value;
                return ss.str();
            }

            // print value with specific precision
            template<class T>
            static std::string printValueWithPrecision(T value, uint32_t precision) {
                std::stringstream ss;
                ss << std::fixed << std::setprecision(precision) << value;
                return ss.str();
            }
    };
}

#endif
