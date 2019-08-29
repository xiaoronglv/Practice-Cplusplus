#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
rm -rf release
mkdir -p release
cd release
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-5.0 -DCMAKE_CXX_COMPILER=clang++-5.0 -DBUILD_TESTS=OFF -DUSE_TCMALLOC=OFF -DDISABLE_GLOG_CONFIGURE_CHECK=ON ..
make -j40 robin
