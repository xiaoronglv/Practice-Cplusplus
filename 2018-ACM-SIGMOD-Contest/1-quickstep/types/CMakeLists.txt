# Declare micro-libs:
add_library(project_types_Type Type.cpp Type.hpp)

# Link dependencies:
target_link_libraries(project_types_Type
                      glog
                      project_utility_Macros
                      project_utility_meta_TypeList)
