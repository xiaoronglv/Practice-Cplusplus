if (NOT _incdir)
  if (WIN32)
	  set(_incdir ENV INCLUDE)
  elseif (APPLE)
	  set(_incdir ENV INCLUDE CPATH)
  else ()
	  set(_incdir ENV INCLUDE CPATH)
  endif ()
endif ()

if (NOT _libdir)
  if (WIN32)
    set(_libdir ENV LIB)
  elseif (APPLE)
    set(_libdir ENV DYLD_LIBRARY_PATH)
  else ()
    set(_libdir ENV LD_LIBRARY_PATH)
  endif ()
endif ()

find_path(NUMA_NUMA_INCLUDE_DIR NAMES numa.h
	PATHS
	${_incdir}
	/usr/include
	/usr/local/include
	/opt/local/include	#Macports
  )

set(NUMA_NAMES numa)
find_library(NUMA_LIBRARY NAMES ${NUMA_NAMES} PATHS ${_libdir} )

if (NUMA_LIBRARY AND NUMA_NUMA_INCLUDE_DIR)
	SET(NUMA_LIBRARIES ${NUMA_LIBRARY})
	SET(NUMA_INCLUDE_DIR ${NUMA_NUMA_INCLUDE_DIR})
	SET(NUMA_FOUND TRUE)
else (NUMA_LIBRARY AND NUMA_NUMA_INCLUDE_DIR) 
	SET(NUMA_FOUND FALSE) 
endif (NUMA_LIBRARY AND NUMA_NUMA_INCLUDE_DIR)


# handle the QUIETLY and REQUIRED arguments and set SLICOT_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NUMA DEFAULT_MSG NUMA_LIBRARY )

mark_as_advanced(NUMA_LIBRARY NUMA_NUMA_INCLUDE_DIR )
