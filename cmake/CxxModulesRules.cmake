if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 16)
    message (FATAL "C++20 module needs Clang++-16 or up")
  endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
    message (FATAL "C++20 module needs g++-14 or up")
  endif ()
else ()
  message (FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif ()

if (CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
  # CMake 3.28 has official support of C++20 modules
elseif (CMAKE_VERSION VERSION_GREATER_EQUAL 3.27)
  set (CMAKE_EXPERIMENTAL_CXX_MODULE_CMAKE_API "aa1f7df0-828a-4fcd-9afc-2dc80491aca7")
elseif (CMAKE_VERSION VERSION_GREATER_EQUAL 3.26)
  set (CMAKE_EXPERIMENTAL_CXX_SCANDEP_SOURCE "")
  set (CMAKE_EXPERIMENTAL_CXX_MODULE_DYNDEP 1)
  set (CMAKE_EXPERIMENTAL_CXX_MODULE_CMAKE_API "2182bf5c-ef0d-489a-91da-49dbc3090d2a")
endif ()

set (CMAKE_CXX_STANDARD_REQUIRED ON)
# C++ extension does work with C++ module support so far
set (CMAKE_CXX_EXTENSIONS OFF)
