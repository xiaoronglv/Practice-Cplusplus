# SIGMOD Programming Contest 2018 Submission
# Copyright (C) 2018  Florian Wolf, Michael Brendle, Georgios Psaropoulos
#
# This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program; if not, see
# <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 3.9)
project (ProgrammingContest CXX)

include(CheckIPOSupported)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)
set(LINK_TO_VTUNE OFF)

find_package(NUMA)

if(${LINK_TO_VTUNE})
#Needs amplxe-cl to be in $PATH
    find_package(VTune)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(src)
if(${LINK_TO_VTUNE})
    include_directories(${VTune_INCLUDE_DIR})
endif()

if(UNIX)
    include_directories(${NUMA_INCLUDE_DIR})
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native -mtune=native")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -march=native -mtune=native")
    add_compile_options(
        -W -Wall -Wextra -Wnon-virtual-dtor
        -Wno-deprecated-declarations -pthread)
    if(${LINK_TO_VTUNE})
        add_definitions(-DVTUNE_PROFILE)
    endif()

    if("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
        add_compile_options(-fcoroutines-ts)
        check_ipo_supported(RESULT result OUTPUT error)
        message(STATUS "LTO supported: ${result}")
        if(result)
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)
        endif()
    else()
        message(WARNING "No coroutines on GCC!")
    endif()
    add_definitions(-DNO_CORO)
else()
    #message(FATAL_ERROR "Only UNIX is supported")
endif()

add_executable(tests_programming_contest
    src/basis/Numa.cpp
    src/basis/TaskScheduler.cpp
    src/database/ColumnPartition.cpp
    src/query_processor/Batch.cpp
    src/query_processor/PrefixBitmap.cpp
    src/tests/TestsProgrammingContest.cpp
)

if(UNIX)
    target_link_libraries(tests_programming_contest jemalloc  "-pthread -lgcc -lm -lpthread -ldl")
    #target_link_libraries(tests_programming_contest numa)
endif()
if(${LINK_TO_VTUNE})
    target_link_libraries(tests_programming_contest ${VTune_LIBRARIES})
endif()
