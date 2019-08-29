cmake_minimum_required (VERSION 2.8.6)
project (PROJECT)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

include(CheckCXXCompilerFlag)
include(CheckCXXSourceCompiles)

macro (set_gflags_lib_name)
  if (BUILD_SHARED_LIBS)
    set(GFLAGS_LIB_NAME gflags_nothreads-shared)
  else()
    set(GFLAGS_LIB_NAME gflags_nothreads-static)
  endif()
endmacro (set_gflags_lib_name)

set_gflags_lib_name ()

# Link against the system's threading library.
find_package(Threads REQUIRED)
set(LIBS ${LIBS} ${CMAKE_THREAD_LIBS_INIT})
set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

# Automatically optimize for host processor.
CHECK_CXX_COMPILER_FLAG("-march=native" CLANG_HAS_MARCH_NATIVE)
if(CLANG_HAS_MARCH_NATIVE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

# Try to find the thread-affinitization function.
CHECK_CXX_SOURCE_COMPILES("
  #ifndef _GNU_SOURCE
  #define _GNU_SOURCE 1
  #endif
  #include <pthread.h>
  #include <sched.h>

  int main() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
      return 1;
    }
    return 0;
  }
  " PROJECT_HAVE_PTHREAD_SETAFFINITY_NP_LINUX)

# Look for GCC-style builtins for bitwise operations.
CHECK_CXX_SOURCE_COMPILES("
  int main() {
    return __builtin_popcount(1234);
  }
  " PROJECT_HAVE_BUILTIN_POPCOUNT)

CHECK_CXX_SOURCE_COMPILES("
  int main() {
    return __builtin_clz(1234);
  }
  " PROJECT_HAVE_BUILTIN_CLZ)

CHECK_CXX_SOURCE_COMPILES("
  int main() {
    return __builtin_ctz(1234);
  }
  " PROJECT_HAVE_BUILTIN_CTZ)

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/ProjectConfig.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/ProjectConfig.h"
)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(THIRD_PARTY_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party")
include(ExternalProject)

# Optional TCMALLOC.
if(UNIX AND NOT CYGWIN AND NOT APPLE)
  option(USE_TCMALLOC "Use tcmalloc (Google's thread-cacheing malloc) instead of system-provided malloc" ON)
else()
  option(USE_TCMALLOC "Use tcmalloc (Google's thread-cacheing malloc) instead of system-provided malloc" OFF)
endif()

if(USE_TCMALLOC)
  # Build tcmalloc as a cmake ExternalProject.
  ExternalProject_Add(
    libtcmalloc_ext
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/gperftools
    SOURCE_DIR ${THIRD_PARTY_SOURCE_DIR}/gperftools
    CONFIGURE_COMMAND <SOURCE_DIR>/configure
        --prefix=<INSTALL_DIR>
        --enable-minimal
        --disable-debugalloc
        CC=${CMAKE_C_COMPILER}
        CXX=${CMAKE_CXX_COMPILER}
        CFLAGS=${THIRD_PARTY_C_FLAGS}
        CXXFLAGS=${THIRD_PARTY_CXX_FLAGS}
    BUILD_COMMAND make -j40
    BUILD_IN_SOURCE 0
  )

  add_library(libtcmalloc_minimal STATIC IMPORTED)
  set_property(TARGET libtcmalloc_minimal PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/third_party/gperftools/lib/libtcmalloc_minimal.a)

  # Linking against tcmalloc also requires linking against the system threading
  # library.
  set_property(TARGET libtcmalloc_minimal PROPERTY IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  add_dependencies(libtcmalloc_minimal libtcmalloc_ext)
  set(LIBS ${LIBS} libtcmalloc_minimal)
endif()

OPTION(BUILD_TESTS "Build tests." ON)

add_subdirectory ("${THIRD_PARTY_SOURCE_DIR}/gflags" "${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include")

add_subdirectory("${THIRD_PARTY_SOURCE_DIR}/glog" "${CMAKE_CURRENT_BINARY_DIR}/third_party/glog")
include_directories(${THIRD_PARTY_SOURCE_DIR}/glog/src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party)

if(BUILD_TESTS)
  add_subdirectory("${THIRD_PARTY_SOURCE_DIR}/googletest" "${CMAKE_CURRENT_BINARY_DIR}/third_party/googletest")
  include_directories("${THIRD_PARTY_SOURCE_DIR}/googletest/googlemock/include")
  include_directories("${THIRD_PARTY_SOURCE_DIR}/googletest/googletest/include")
  enable_testing()
endif()

add_subdirectory(operators)
add_subdirectory(optimizer)
add_subdirectory(partition)
add_subdirectory(scheduler)
add_subdirectory(storage)
add_subdirectory(threading)
add_subdirectory(types)
add_subdirectory(utility)
add_subdirectory(workload)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

add_executable(harness Harness.cpp)
add_executable(robin Main.cpp)

target_link_libraries(robin
                      ${GFLAGS_LIB_NAME}
                      ${LIBS}
                      glog
                      project_scheduler_Scheduler
                      project_scheduler_TaskStatistics
                      project_utility_EventProfiler
                      project_workload_Workload)
