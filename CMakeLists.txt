cmake_minimum_required(VERSION 3.17)
project(uLayFS)

set(CMAKE_CXX_STANDARD 17)

add_library(ulayfs SHARED src/lib.cpp)

add_executable(basic_test test/basic.c)
target_link_libraries(basic_test ulayfs)
