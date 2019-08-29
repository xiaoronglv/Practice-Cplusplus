#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
mkdir -p build
cd build

# export CXX=g++
export CXX="clang++-5.0 -flto"
# cmake -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles" ..
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXE_LINKER_FLAGS="-flto" -G "Unix Makefiles" ..
# cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXE_LINKER_FLAGS="-flto" -G "Unix Makefiles" ..
make -j
