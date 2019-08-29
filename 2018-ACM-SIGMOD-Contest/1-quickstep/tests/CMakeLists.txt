# Declare micro-libs:
add_executable(project_tests_BitVectorBenchmark BitVectorBenchmark.cpp)
add_executable(project_tests_PartitionTest PartitionTest.cpp)
add_executable(project_tests_RangeTest RangeTest.cpp)
add_executable(project_tests_SchedulerTest SchedulerTest.cpp)

# Link dependencies:
target_link_libraries(project_tests_PartitionTest
                      ${GFLAGS_LIB_NAME}
                      glog
                      project_operators_aggregators_BuildKeyCountVector
                      project_operators_utility_KeyCountVector
                      project_partition_PartitionDestination
                      project_partition_PartitionExecutor
                      project_partition_RangeSegmentation
                      project_scheduler_Scheduler
                      project_scheduler_Task
                      project_storage_ColumnVector
                      project_storage_GenericAccessor
                      project_types_Type
                      project_utility_EventProfiler
                      project_utility_Range)
target_link_libraries(project_tests_RangeTest
                      project_utility_Range
                      project_utility_StringUtil)
target_link_libraries(project_tests_SchedulerTest
                      ${GFLAGS_LIB_NAME}
                      glog
                      project_scheduler_Scheduler
                      project_scheduler_Task
                      project_utility_EventProfiler
                      project_utility_Range)
