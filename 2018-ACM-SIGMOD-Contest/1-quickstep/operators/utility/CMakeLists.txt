# Declare micro-libs:
add_library(project_operators_utility_ForeignKeySlotAccessor ../../empty_src.cpp ForeignKeySlotAccessor.hpp)
add_library(project_operators_utility_KeyCountVector ../../empty_src.cpp KeyCountVector.hpp)
add_library(project_operators_utility_KeyCountVectorAccumulator KeyCountVectorAccumulator.cpp KeyCountVectorAccumulator.hpp)
add_library(project_operators_utility_KeyCountVectorAdaptiveAccumulator KeyCountVectorAdaptiveAccumulator.cpp KeyCountVectorAdaptiveAccumulator.hpp)
add_library(project_operators_utility_KeyCountVectorChain ../../empty_src.cpp KeyCountVectorChain.hpp)
add_library(project_operators_utility_MultiwayJoinContext ../../empty_src.cpp MultiwayJoinContext.hpp)

# Link dependencies:
target_link_libraries(project_operators_utility_ForeignKeySlotAccessor
                      glog
                      project_storage_StorageTypedefs
                      project_utility_Macros)
target_link_libraries(project_operators_utility_KeyCountVector
                      glog
                      project_scheduler_Task
                      project_storage_ExistenceMap
                      project_utility_Macros
                      project_utility_Range
                      project_utility_meta_TypeList)
target_link_libraries(project_operators_utility_KeyCountVectorAccumulator
                      glog
                      project_operators_specializations_KeyCountVectorAccumulator00
                      project_operators_specializations_KeyCountVectorAccumulator01
                      project_operators_specializations_KeyCountVectorAccumulator02
                      project_operators_specializations_KeyCountVectorAccumulator03
                      project_operators_specializations_KeyCountVectorAccumulatorCommon
                      project_operators_utility_MultiwayJoinContext
                      project_scheduler_Task
                      project_utility_HashFilter
                      project_utility_Macros)
target_link_libraries(project_operators_utility_KeyCountVectorChain
                      project_utility_Macros)
target_link_libraries(project_operators_utility_MultiwayJoinContext
                      project_operators_expressions_Predicate
                      project_operators_utility_KeyCountVector
                      project_scheduler_Task
                      project_storage_Relation
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_utility_HashFilter)
