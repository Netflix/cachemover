cmake_minimum_required(VERSION 3.3)
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
# Add gprof.
# TODO: Make configurable
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")

set(CMAKE_BUILD_TYPE Debug)

add_executable(memcache-dumper ${PROJECT_SOURCE_DIR}/dumper-main.cc)
target_link_libraries(memcache-dumper common dumper spdlog::spdlog tasks utils)

add_subdirectory(common)
add_subdirectory(dumper)
add_subdirectory(extern/spdlog)
add_subdirectory(tasks)
add_subdirectory(utils)
