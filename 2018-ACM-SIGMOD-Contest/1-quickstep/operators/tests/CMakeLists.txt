# Declare micro-libs:
if(BUILD_TESTS)
  add_executable(project_operators_tests_Comparison_test Comparison_test.cpp)
endif()

# Link dependencies:
if(BUILD_TESTS)
  target_link_libraries(project_operators_tests_Comparison_test
                        gtest
                        gtest_main
                        project_operators_expressions_Comparison
                        project_operators_expressions_ScalarAttribute
                        project_operators_expressions_ScalarLiteral
                        project_storage_Attribute
                        project_storage_Relation
                        project_types_Type
                        project_utility_Range)
endif()

# Tests:
if(BUILD_TESTS)
  add_test(project_operators_tests_Comparison_test project_operators_tests_Comparison_test)
endif()
