# Install script for directory: /home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/lib/libgflags.a")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/lib/libgflags_nothreads.a")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/gflags" TYPE FILE FILES
    "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/include/gflags/gflags.h"
    "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/include/gflags/gflags_declare.h"
    "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/include/gflags/gflags_completions.h"
    "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/include/gflags/gflags_gflags.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags" TYPE FILE RENAME "gflags-config.cmake" FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/gflags-config-install.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags" TYPE FILE FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/gflags-config-version.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags/gflags-export.cmake")
    file(DIFFERENT EXPORT_FILE_CHANGED FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags/gflags-export.cmake"
         "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/CMakeFiles/Export/lib/cmake/gflags/gflags-export.cmake")
    if(EXPORT_FILE_CHANGED)
      file(GLOB OLD_CONFIG_FILES "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags/gflags-export-*.cmake")
      if(OLD_CONFIG_FILES)
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags/gflags-export.cmake\" will be replaced.  Removing files [${OLD_CONFIG_FILES}].")
        file(REMOVE ${OLD_CONFIG_FILES})
      endif()
    endif()
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags" TYPE FILE FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/CMakeFiles/Export/lib/cmake/gflags/gflags-export.cmake")
  if("${CMAKE_INSTALL_CONFIG_NAME}" MATCHES "^()$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/gflags" TYPE FILE FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/CMakeFiles/Export/lib/cmake/gflags/gflags-export-noconfig.cmake")
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/xiaoronglv/Workspace/Practice-Cplusplus/2018-ACM-SIGMOD-Contest/1-quickstep/third_party/gflags/src/gflags_completions.sh")
endif()

