# Declare micro-libs:
add_library(project_partition_PartitionBlockAllocator ../empty_src.cpp PartitionBlockAllocator.hpp)
add_library(project_partition_PartitionChain ../empty_src.cpp PartitionChain.hpp)
add_library(project_partition_PartitionDestination ../empty_src.cpp PartitionDestination.hpp)
add_library(project_partition_PartitionDestinationBase ../empty_src.cpp PartitionDestinationBase.hpp)
add_library(project_partition_PartitionExecutor ../empty_src.cpp PartitionExecutor.hpp)
add_library(project_partition_PartitionGroup ../empty_src.cpp PartitionGroup.hpp)
add_library(project_partition_RangeSegmentation ../empty_src.cpp RangeSegmentation.hpp)

# Link dependencies:
target_link_libraries(project_partition_PartitionBlockAllocator
                      project_utility_Macros
                      project_utility_ScopedArray)
target_link_libraries(project_partition_PartitionChain
                      glog
                      project_partition_PartitionBlockAllocator
                      project_utility_Macros)
target_link_libraries(project_partition_PartitionDestination
                      project_partition_PartitionDestinationBase
                      project_partition_PartitionGroup
                      project_utility_Macros)
target_link_libraries(project_partition_PartitionDestinationBase
                      project_utility_Macros)
target_link_libraries(project_partition_PartitionExecutor
                      project_partition_PartitionDestination
                      project_scheduler_Task
                      project_utility_Macros)
target_link_libraries(project_partition_PartitionGroup
                      glog
                      project_partition_PartitionBlockAllocator
                      project_partition_PartitionChain
                      project_utility_Macros)
target_link_libraries(project_partition_RangeSegmentation
                      glog
                      project_utility_BitManipulation
                      project_utility_Range)
