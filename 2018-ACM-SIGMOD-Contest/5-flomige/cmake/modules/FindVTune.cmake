# - Find VTune ittnotify.
# Defines:
# VTune_FOUND
# VTune_INCLUDE_DIRS
# VTune_LIBRARIES

find_program(VTUNE_EXECUTABLE amplxe-cl)
get_filename_component(VTUNE_DIR ${VTUNE_EXECUTABLE} PATH)

set(VTUNE_DIR "${VTUNE_DIR}/..")

find_path(VTune_INCLUDE_DIR ittnotify.h
    PATHS ${VTUNE_DIR}
    PATH_SUFFIXES include)

if (CMAKE_SIZEOF_VOID_P MATCHES "8")
  set(vtune_lib_dir lib64)
else()
  set(vtune_lib_dir lib32)
endif()

find_library(VTune_LIBRARIES libittnotify.a
    PATHS ${VTUNE_DIR}
    PATH_SUFFIXES ${vtune_lib_dir})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    VTune DEFAULT_MSG VTune_LIBRARIES VTune_INCLUDE_DIR)