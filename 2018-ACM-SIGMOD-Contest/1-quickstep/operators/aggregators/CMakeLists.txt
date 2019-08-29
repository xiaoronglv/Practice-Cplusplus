# Declare micro-libs:
add_library(project_operators_aggregators_AggregationIsSorted ../../empty_src.cpp AggregationIsSorted.hpp)
add_library(project_operators_aggregators_AggregationMinMax ../../empty_src.cpp AggregationMinMax.hpp)
add_library(project_operators_aggregators_AggregationSum ../../empty_src.cpp AggregationSum.hpp)
add_library(project_operators_aggregators_BuildExistenceMap ../../empty_src.cpp BuildExistenceMap.hpp)
add_library(project_operators_aggregators_BuildForeignKeyIndex ../../empty_src.cpp BuildForeignKeyIndex.hpp)
add_library(project_operators_aggregators_BuildKeyCountVector BuildKeyCountVector.cpp BuildKeyCountVector.hpp)
add_library(project_operators_aggregators_BuildPrimaryKeyIndex ../../empty_src.cpp BuildPrimaryKeyIndex.hpp)
add_library(project_operators_aggregators_BuildRangedPartitions BuildRangedPartitions.cpp BuildRangedPartitions.hpp)
add_library(project_operators_aggregators_CompressColumn ../../empty_src.cpp CompressColumn.hpp)

# Link dependencies:
target_link_libraries(project_operators_aggregators_AggregationIsSorted
                      glog
                      project_scheduler_Task
                      project_storage_ColumnAccessor
                      project_storage_ColumnStoreBlock
                      project_storage_Relation
                      project_storage_StorageBlock
                      project_types_Type
                      project_utility_Macros)
target_link_libraries(project_operators_aggregators_AggregationMinMax
                      project_scheduler_Task
                      project_storage_ColumnAccessor
                      project_storage_Relation
                      project_utility_Macros)
target_link_libraries(project_operators_aggregators_BuildExistenceMap
                      glog
                      project_partition_PartitionDestination
                      project_partition_PartitionDestinationBase
                      project_scheduler_Task
                      project_storage_ExistenceMap
                      project_storage_IndexManager
                      project_storage_Relation
                      project_types_Type
                      project_utility_Macros
                      project_utility_Range
                      project_utility_ThreadSafePool)
target_link_libraries(project_operators_aggregators_BuildForeignKeyIndex
                      glog
                      project_operators_utility_KeyCountVector
                      project_scheduler_Task
                      project_storage_AttributeStatistics
                      project_storage_ForeignKeyIndex
                      project_storage_Relation
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_MultiwayParallelMergeSort
                      project_utility_Range
                      project_utility_ScopedArray)
target_link_libraries(project_operators_aggregators_BuildKeyCountVector
                      glog
                      project_operators_utility_KeyCountVector
                      project_partition_PartitionDestination
                      project_partition_PartitionDestinationBase
                      project_partition_PartitionExecutor
                      project_partition_RangeSegmentation
                      project_scheduler_Task
                      project_storage_AttributeStatistics
                      project_storage_ColumnAccessor
                      project_storage_GenericAccessor
                      project_storage_IndexManager
                      project_storage_Relation
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_types_Type
                      project_utility_BitManipulation
                      project_utility_EventProfiler
                      project_utility_HashFilter
                      project_utility_Macros
                      project_utility_Range
                      project_utility_ThreadSafePool)
target_link_libraries(project_operators_aggregators_BuildPrimaryKeyIndex
                      glog
                      project_partition_PartitionDestination
                      project_partition_PartitionDestinationBase
                      project_scheduler_Task
                      project_storage_AttributeStatistics
                      project_storage_IndexManager
                      project_storage_PrimaryKeyIndex
                      project_storage_Relation
                      project_storage_StorageTypedefs
                      project_types_Type
                      project_utility_EventProfiler
                      project_utility_Macros
                      project_utility_ScopedArray)
target_link_libraries(project_operators_aggregators_BuildRangedPartitions
                      project_partition_PartitionDestinationBase
                      project_partition_PartitionExecutor
                      project_partition_RangeSegmentation
                      project_scheduler_Task
                      project_storage_AttributeStatistics
                      project_storage_IndexManager
                      project_storage_Relation
                      project_storage_StorageTypedefs
                      project_types_Type
                      project_utility_BitManipulation
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_operators_aggregators_CompressColumn
                      glog
                      project_scheduler_Task
                      project_storage_ColumnVector
                      project_storage_Relation
                      project_types_Type
                      project_utility_Macros)
