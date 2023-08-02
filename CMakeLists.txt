cmake_minimum_required(VERSION 3.4)
project(cs564_project_3)

set(CMAKE_CXX_STANDARD 17)

enable_testing()

add_library(sqlite src/extern/sqlite3.h src/extern/sqlite3.c)
target_compile_options(
        sqlite
        PRIVATE
        "-DSQLITE_THREADSAFE=0"
        "-DSQLITE_OMIT_LOAD_EXTENSION"
)

add_library(
        join
        src/join.cpp
        src/join.hpp
        src/file.cpp
        src/file.hpp
)
target_link_libraries(join sqlite)

add_executable(test_file src/test/test_file.cpp)
target_include_directories(test_file PRIVATE src)
target_link_libraries(test_file join)
add_test(test_file test_file)

add_executable(test_join src/test/test_join.cpp)
target_include_directories(test_join PRIVATE src)
target_link_libraries(test_join join)
add_test(test_join test_join)

add_executable(test_memory_setup src/test/memory/test_memory_setup.cpp)
target_include_directories(test_memory_setup PRIVATE src)
target_link_libraries(test_memory_setup join)

add_executable(test_memory src/test/memory/test_memory.cpp)
target_include_directories(test_memory PRIVATE src)
target_link_libraries(test_memory join)

file(COPY src/test/memory/test_memory.sh DESTINATION ${CMAKE_BINARY_DIR})
