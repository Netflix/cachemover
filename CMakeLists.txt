cmake_minimum_required(VERSION 3.9)
project(MemcacheDumper)

# Set the output folder for executables
set(CMAKE_BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/bin)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

include_directories("${PROJECT_SOURCE_DIR}")
include_directories("${PROJECT_SOURCE_DIR}/dumper")

# Use C++17
set(CMAKE_CXX_STANDARD 17)
# Enable all compiler warnings.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
# Don't follow strict aliasing rules.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-strict-aliasing")
# Add dependency for pthreads.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
# Add dependency for std::experimental::filesystem.
# TODO: Move to c++17 to use std::filesystem instead.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lstdc++fs")

option(GPROF_ENABLED "Enable GPROF instrumentation" OFF)
option(ASAN_ENABLED "Enable ASAN" OFF)

if (GPROF_ENABLED)
  message("NOTE: Building with GPROF")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
endif(GPROF_ENABLED)

if(ASAN_ENABLED)
  message("NOTE: Building with ASAN")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif(ASAN_ENABLED)

set(CMAKE_BUILD_TYPE Debug)

# RapidJSON for metrics reporting
find_package(RapidJSON REQUIRED)
# Curl for URL decoding
find_package(CURL REQUIRED)
include_directories(SYSTEM ${CURL_INCLUDE_DIR})

# Pistache for an async REST Server
set(PISTACHE_INCLUDE_DIR "extern/pistache/include")
set(PISTACHE_LIBRARY "${PROJECT_SOURCE_DIR}/extern/pistache/libpistache.a")
include_directories(${PISTACHE_INCLUDE_DIR})

add_executable(memcache-dumper ${PROJECT_SOURCE_DIR}/dumper-main.cc)
target_link_libraries(
  memcache-dumper
  common
  dumper
  tasks
  utils
  stdc++fs
  ${CURL_LIBRARIES}
  ${PISTACHE_LIBRARY}
  spdlog::spdlog)

add_subdirectory(common)
add_subdirectory(dumper)
add_subdirectory(extern/spdlog)
add_subdirectory(tasks)
add_subdirectory(utils)
