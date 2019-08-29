# Declare micro-libs:
add_library(project_workload_TableAnalyzer TableAnalyzer.cpp TableAnalyzer.hpp)
add_library(project_workload_Workload Workload.cpp Workload.hpp)

# Link dependencies:
target_link_libraries(project_workload_TableAnalyzer
                      ${GFLAGS_LIB_NAME}
                      project_operators_aggregators_AggregationIsSorted
                      project_operators_aggregators_AggregationMinMax
                      project_operators_aggregators_BuildExistenceMap
                      project_operators_aggregators_BuildForeignKeyIndex
                      project_operators_aggregators_BuildKeyCountVector
                      project_operators_aggregators_BuildPrimaryKeyIndex
                      project_operators_aggregators_BuildRangedPartitions
                      project_operators_aggregators_CompressColumn
                      project_scheduler_Task
                      project_storage_Attribute
                      project_storage_AttributeStatistics
                      project_storage_Database
                      project_storage_ExistenceMap
                      project_storage_Relation
                      project_types_Type
                      project_utility_EventProfiler
                      project_utility_Macros
                      project_utility_SyncStream)
target_link_libraries(project_workload_Workload
                      ${GFLAGS_LIB_NAME}
                      project_operators_utility_KeyCountVector
                      project_optimizer_Optimizer
                      project_optimizer_QueryHandle
                      project_scheduler_Scheduler
                      project_scheduler_Task
                      project_storage_AttributeStatistics
                      project_storage_ColumnStoreBlock
                      project_storage_ColumnVector
                      project_storage_Database
                      project_storage_ForeignKeyIndex
                      project_storage_IndexManager
                      project_storage_PrimaryKeyIndex
                      project_storage_Relation
                      project_storage_RelationStatistics
                      project_storage_StorageTypedefs
                      project_threading_SynchronizationLock
                      project_types_Type
                      project_utility_EventProfiler
                      project_utility_Macros
                      project_utility_ScopedArray
                      project_workload_TableAnalyzer)
