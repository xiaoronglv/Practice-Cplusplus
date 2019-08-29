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

#ifndef NUMA_ALLOCATOR_H
#define NUMA_ALLOCATOR_H

#include <limits>
#include <stdexcept>
#include <vector>

namespace basis{

    static constexpr const uint32_t NUMA_NODE_COUNT = 1;
    static constexpr const uint32_t CORES_PER_NUMA = 39;

    class NumaInterface{
        public:
            // used by workers to bind their execution and mem alloc to a numa node
            static void bindCurrentContextToNode(uint32_t nodeNumber);

            static void* allocOnNode(std::size_t numberOfBytes, uint32_t numaNode);

            static void numaFree(void* mem, std::size_t numberOfBytes);
    };


    // mostly for table and column partitions to overload their 'new' operator
    class NumaAllocated{
        public:
            NumaAllocated();

            virtual ~NumaAllocated();

            void* operator new(std::size_t numberOfBytes, uint32_t numaNode);

            void operator delete(void* mem, std::size_t numberOfBytes);

            void* operator new[](std::size_t);

            void operator delete[](void*);
    };


    // for stl containers to allocate their payload memory on a numa node
    template <class T>
    class NumaAllocator{

        private:
            uint32_t _numaNode;

        public:
            using pointer=T*;
            using const_pointer=const T*;
            using value_type=T;
            using reference=T&;
            using const_reference=const T&;
            using size_type=std::size_t;

            NumaAllocator(uint32_t numaNode) : _numaNode(numaNode){
                // check if numa node is avaiable
                // TODO
            }

            template <class U>
            NumaAllocator(const NumaAllocator<U>& other){
                _numaNode = other.getNumaNode();
            }

            T* allocate(std::size_t numberOfTs){
                std::size_t numberOfBytes = numberOfTs * sizeof(T);
                void* mem = NumaInterface::allocOnNode(numberOfBytes, _numaNode);
                return static_cast<T*>(mem);
            }

            void deallocate(T* mem, std::size_t numberOfTs){
                std::size_t numberOfBytes = numberOfTs * sizeof(T);
                NumaInterface::numaFree(mem,numberOfBytes);
            }

            template<class Other>
            struct rebind { typedef NumaAllocator<Other> other; };

            template< class U >
            void destroy( U* p ){
                p->~U();
            }

            template< class U, class... Args >
            void construct( U* p, Args&&... args ){
                ::new((void *)p) U(std::forward<Args>(args)...);
            }

            size_type max_size() const{
                return std::numeric_limits<size_type>::max();
            }

            uint32_t getNumaNode() const {
                return _numaNode;
            }

    };

    template <class T, class U>
    bool operator==(const NumaAllocator<T>& t, const NumaAllocator<U>& u){
        return t.getNumaNode()==u.getNumaNode();
    }

    template <class T, class U>
    bool operator!=(const NumaAllocator<T>& t , const NumaAllocator<U>& u){
        return t.getNumaNode()!=u.getNumaNode();
    }

}

#endif
