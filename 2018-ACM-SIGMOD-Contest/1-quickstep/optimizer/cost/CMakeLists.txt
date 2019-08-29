# Declare micro-libs:
add_library(project_optimizer_cost_SimpleCostModel SimpleCostModel.cpp SimpleCostModel.hpp)

# Link dependencies:
target_link_libraries(project_optimizer_cost_SimpleCostModel
                      glog
                      project_optimizer_AttributeReference
                      project_optimizer_ChainedJoinAggregate
                      project_optimizer_Comparison
                      project_optimizer_Conjunction
                      project_optimizer_EquiJoin
                      project_optimizer_EquiJoinAggregate
                      project_optimizer_ExprId
                      project_optimizer_Expression
                      project_optimizer_ExpressionUtil
                      project_optimizer_MultiwayEquiJoinAggregate
                      project_optimizer_PatternMatcher
                      project_optimizer_Plan
                      project_optimizer_Predicate
                      project_optimizer_PredicateLiteral
                      project_optimizer_ScalarLiteral
                      project_optimizer_Selection
                      project_optimizer_TableReference
                      project_optimizer_TableView
                      project_optimizer_TopLevelPlan
                      project_storage_Attribute
                      project_storage_AttributeStatistics
                      project_storage_Database
                      project_storage_IndexManager
                      project_storage_Relation
                      project_storage_RelationStatistics
                      project_storage_StorageTypedefs
                      project_utility_Macros
                      project_utility_Range)
