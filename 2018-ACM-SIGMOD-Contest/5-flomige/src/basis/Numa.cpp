// SIGMOD Programming Contest 2018 Submission
// Copyright (C) 2018  Florian Wolf, Michael Brendle, Georgios Psaropoulos
//
// This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with this program; if not, see
// <http://www.gnu.org/licenses/>.

#include "Numa.h"

#ifdef _WIN32
#include <Windows.h>
#else
// #include <numa.h>
#endif
#include <stdlib.h>

namespace basis{

    //
    // NumaInterface
    //

    void NumaInterface::bindCurrentContextToNode(uint32_t ){ // nodeNumber
// #ifdef _WIN32
// 		HANDLE hThread = GetCurrentThread();
// 		ULONGLONG nodemask;
// 		if (!GetNumaNodeProcessorMask((UCHAR)nodeNumber, &nodemask)) {
// 			throw std::runtime_error("could not get affinity mask for node " + nodeNumber);
// 		}
// 		auto oldmask = SetThreadAffinityMask(hThread, nodemask);

// 		if (oldmask == 0) {
// 			throw std::runtime_error("could not bind current thread to node " + nodeNumber);
// 		}
// #else
// 		bitmask* nodemask = numa_allocate_nodemask(); // zero filled per definition
//         nodemask = numa_bitmask_setbit(nodemask, static_cast<int>(nodeNumber));
//         numa_bind(nodemask);
// #endif // _WIN32
    }

    void* NumaInterface::allocOnNode(std::size_t numberOfBytes, uint32_t ){ //numaNode
        // allocate
        // void* mem = numa_alloc_onnode(numberOfBytes, numaNode);
        void* mem = malloc(numberOfBytes);
        // return on success
        if(mem == nullptr){
            throw std::bad_alloc();
        }
        return mem;
    }

    void NumaInterface::numaFree(void* mem, std::size_t ){ //numberOfBytes
        // forward to 'numa_free'
        // numa_free(mem,numberOfBytes);
        free(mem);
    }


    //
    // NumaAllocated
    //

    NumaAllocated::NumaAllocated(){
        // check if numa available - constructor of the static member in the numa interface does that
    }

    NumaAllocated::~NumaAllocated(){
    }

    void* NumaAllocated::operator new(std::size_t numberOfBytes, uint32_t numaNode){
        // check if numa node is avaiable
        // TODO
        // allocate memory
        return NumaInterface::allocOnNode(numberOfBytes, numaNode);
    }

    void NumaAllocated::operator delete(void* mem, std::size_t numberOfBytes){
        // forward to numa interface
        NumaInterface::numaFree(mem, numberOfBytes);
    }

    void* NumaAllocated::operator new[](std::size_t){
        throw std::logic_error("Array allocator not implemented");
    }

    void NumaAllocated::operator delete[](void*){
        throw std::logic_error("Array deallocator not implemented");
    }


}
