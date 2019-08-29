# Declare micro-libs:
add_library(project_operators_expressions_Comparison Comparison.cpp Comparison.hpp)
add_library(project_operators_expressions_ComparisonType ../../empty_src.cpp ComparisonType.hpp)
add_library(project_operators_expressions_Conjunction Conjunction.cpp Conjunction.hpp)
add_library(project_operators_expressions_ExpressionSynthesizer ../../empty_src.cpp ExpressionSynthesizer.hpp)
add_library(project_operators_expressions_Predicate ../../empty_src.cpp Predicate.hpp)
add_library(project_operators_expressions_Scalar ../../empty_src.cpp Scalar.hpp)
add_library(project_operators_expressions_ScalarAttribute ScalarAttribute.cpp ScalarAttribute.hpp)
add_library(project_operators_expressions_ScalarLiteral ../../empty_src.cpp ScalarLiteral.hpp)
add_library(project_operators_expressions_TruePredicate TruePredicate.cpp TruePredicate.hpp)

# Link dependencies:
target_link_libraries(project_operators_expressions_Comparison
                      glog
                      project_operators_expressions_ComparisonType
                      project_operators_expressions_Predicate
                      project_operators_expressions_Scalar
                      project_operators_expressions_ScalarAttribute
                      project_operators_expressions_ScalarLiteral
                      project_storage_ColumnAccessor
                      project_storage_ColumnStoreBlock
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_types_Type
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_operators_expressions_Conjunction
                      glog
                      project_operators_expressions_Predicate
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_utility_Range)
target_link_libraries(project_operators_expressions_ExpressionSynthesizer
                      project_operators_expressions_Comparison
                      project_operators_expressions_Conjunction
                      project_operators_expressions_Predicate
                      project_operators_expressions_Scalar
                      project_operators_expressions_ScalarAttribute
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_types_Type
                      project_utility_Macros)
target_link_libraries(project_operators_expressions_Predicate
                      glog
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range)
target_link_libraries(project_operators_expressions_Scalar
                      project_storage_TupleIdSequence
                      project_types_Type
                      project_utility_Macros)
target_link_libraries(project_operators_expressions_ScalarAttribute
                      glog
                      project_operators_expressions_Scalar
                      project_storage_Attribute
                      project_storage_ColumnVector
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_utility_Macros)
target_link_libraries(project_operators_expressions_ScalarLiteral
                      glog
                      project_operators_expressions_Scalar
                      project_types_Type
                      project_utility_Macros)
target_link_libraries(project_operators_expressions_TruePredicate
                      glog
                      project_operators_expressions_Predicate
                      project_storage_StorageBlock
                      project_storage_StorageTypedefs
                      project_storage_TupleIdSequence
                      project_utility_Macros
                      project_utility_Range)
