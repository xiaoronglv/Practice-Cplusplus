# Declare micro-libs:
add_library(project_operators_specializations_KeyCountVectorAccumulator00 KeyCountVectorAccumulator00.cpp KeyCountVectorAccumulator00.hpp)
add_library(project_operators_specializations_KeyCountVectorAccumulator01 KeyCountVectorAccumulator01.cpp KeyCountVectorAccumulator01.hpp)
add_library(project_operators_specializations_KeyCountVectorAccumulator02 KeyCountVectorAccumulator02.cpp KeyCountVectorAccumulator02.hpp)
add_library(project_operators_specializations_KeyCountVectorAccumulator03 KeyCountVectorAccumulator03.cpp KeyCountVectorAccumulator03.hpp)
add_library(project_operators_specializations_KeyCountVectorAccumulatorCommon ../../empty_src.cpp KeyCountVectorAccumulatorCommon.hpp)

# Link dependencies:
target_link_libraries(project_operators_specializations_KeyCountVectorAccumulator00
                      project_operators_specializations_KeyCountVectorAccumulatorCommon
                      project_utility_HashFilter
                      project_utility_Macros)
target_link_libraries(project_operators_specializations_KeyCountVectorAccumulator01
                      project_operators_specializations_KeyCountVectorAccumulatorCommon
                      project_operators_utility_KeyCountVector
                      project_utility_HashFilter
                      project_utility_Macros)
target_link_libraries(project_operators_specializations_KeyCountVectorAccumulator02
                      glog
                      project_operators_specializations_KeyCountVectorAccumulatorCommon
                      project_utility_HashFilter
                      project_utility_Macros)
target_link_libraries(project_operators_specializations_KeyCountVectorAccumulator03
                      project_operators_specializations_KeyCountVectorAccumulatorCommon
                      project_utility_HashFilter
                      project_utility_Macros)
target_link_libraries(project_operators_specializations_KeyCountVectorAccumulatorCommon
                      glog
                      project_operators_expressions_Scalar
                      project_operators_utility_KeyCountVector
                      project_operators_utility_KeyCountVectorChain
                      project_operators_utility_MultiwayJoinContext
                      project_storage_ColumnAccessor
                      project_storage_ColumnStoreBlock
                      project_storage_GenericAccessor
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_utility_HashFilter
                      project_utility_Macros
                      project_utility_Range)
