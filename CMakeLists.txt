cmake_minimum_required(VERSION 3.17)
project(uLayFS)

set(CMAKE_CXX_STANDARD 17)

add_library(ulayfs SHARED lib.cpp)

add_executable(test test.c)
target_link_libraries(test ulayfs)
