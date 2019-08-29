# Declare micro-libs:
add_library(project_storage_Attribute ../empty_src.cpp Attribute.hpp)
add_library(project_storage_AttributeStatistics ../empty_src.cpp AttributeStatistics.hpp)
add_library(project_storage_ColumnAccessor ../empty_src.cpp ColumnAccessor.hpp)
add_library(project_storage_ColumnStoreBlock ../empty_src.cpp ColumnStoreBlock.hpp)
add_library(project_storage_ColumnVector ../empty_src.cpp ColumnVector.hpp)
add_library(project_storage_Database ../empty_src.cpp Database.hpp)
add_library(project_storage_ExistenceMap ../empty_src.cpp ExistenceMap.hpp)
add_library(project_storage_ForeignKeyIndex ../empty_src.cpp ForeignKeyIndex.hpp)
add_library(project_storage_GenericAccessor ../empty_src.cpp GenericAccessor.hpp)
add_library(project_storage_IndexManager ../empty_src.cpp IndexManager.hpp)
add_library(project_storage_PrimaryKeyIndex ../empty_src.cpp PrimaryKeyIndex.hpp)
add_library(project_storage_Relation Relation.cpp Relation.hpp)
add_library(project_storage_RelationStatistics ../empty_src.cpp RelationStatistics.hpp)
add_library(project_storage_RowStoreBlock ../empty_src.cpp RowStoreBlock.hpp)
add_library(project_storage_StorageBlock ../empty_src.cpp StorageBlock.hpp)
add_library(project_storage_StorageTypedefs ../empty_src.cpp StorageTypedefs.hpp)
add_library(project_storage_TupleIdSequence ../empty_src.cpp TupleIdSequence.hpp)

# Link dependencies:
target_link_libraries(project_storage_Attribute
                      glog
                      project_storage_StorageTypedefs
                      project_types_Type
                      project_utility_Macros)
target_link_libraries(project_storage_AttributeStatistics
                      glog
                      project_utility_Macros)
target_link_libraries(project_storage_ColumnAccessor
                      glog
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_storage_ColumnStoreBlock
                      glog
                      project_storage_ColumnAccessor
                      project_storage_ColumnVector
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_storage_ColumnVector
                      project_storage_ColumnAccessor
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_storage_Database
                      glog
                      project_storage_Attribute
                      project_storage_Relation
                      project_storage_StorageTypedefs
                      project_utility_Macros)
target_link_libraries(project_storage_ExistenceMap
                      glog
                      project_utility_BitVector
                      project_utility_Range)
target_link_libraries(project_storage_ForeignKeyIndex
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range
                      project_utility_ScopedArray)
target_link_libraries(project_storage_GenericAccessor
                      glog
                      project_storage_ColumnAccessor
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_utility_HashFilter
                      project_utility_Macros)
target_link_libraries(project_storage_IndexManager
                      glog
                      project_operators_utility_KeyCountVector
                      project_partition_PartitionDestinationBase
                      project_storage_ExistenceMap
                      project_storage_ForeignKeyIndex
                      project_storage_PrimaryKeyIndex
                      project_storage_StorageTypedefs
                      project_utility_Macros)
target_link_libraries(project_storage_PrimaryKeyIndex
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range
                      project_utility_ScopedArray)
target_link_libraries(project_storage_Relation
                      glog
                      project_storage_Attribute
                      project_storage_AttributeStatistics
                      project_storage_ColumnAccessor
                      project_storage_ColumnStoreBlock
                      project_storage_IndexManager
                      project_storage_RelationStatistics
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_types_Type
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_storage_RelationStatistics
                      glog
                      project_storage_AttributeStatistics
                      project_storage_StorageTypedefs
                      project_utility_Macros)
target_link_libraries(project_storage_RowStoreBlock
                      glog
                      project_storage_ColumnAccessor
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range
                      project_utility_meta_Common
                      project_utility_meta_TypeList)
target_link_libraries(project_storage_StorageBlock
                      project_storage_ColumnAccessor
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range
                      project_utility_ScopedArray)
target_link_libraries(project_storage_TupleIdSequence
                      glog
                      project_storage_StorageTypedefs
                      project_utility_BitVector
                      project_utility_Macros)
