# Declare micro-libs:
add_library(project_utility_specializations_RowStoreBlockSorter01 RowStoreBlockSorter01.cpp RowStoreBlockSorter01.hpp)
add_library(project_utility_specializations_RowStoreBlockSorter02 RowStoreBlockSorter02.cpp RowStoreBlockSorter02.hpp)
add_library(project_utility_specializations_RowStoreBlockSorter03 RowStoreBlockSorter03.cpp RowStoreBlockSorter03.hpp)
add_library(project_utility_specializations_RowStoreBlockSorterCommon ../../empty_src.cpp RowStoreBlockSorterCommon.hpp)

# Link dependencies:
target_link_libraries(project_utility_specializations_RowStoreBlockSorter01
                      project_types_Type
                      project_utility_specializations_RowStoreBlockSorterCommon)
target_link_libraries(project_utility_specializations_RowStoreBlockSorter02
                      project_types_Type
                      project_utility_specializations_RowStoreBlockSorterCommon)
target_link_libraries(project_utility_specializations_RowStoreBlockSorter03
                      project_types_Type
                      project_utility_specializations_RowStoreBlockSorterCommon)
target_link_libraries(project_utility_specializations_RowStoreBlockSorterCommon
                      project_scheduler_Task
                      project_storage_RowStoreBlock
                      project_types_Type
                      project_utility_MultiwayParallelMergeSort)
