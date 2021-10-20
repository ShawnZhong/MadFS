cmake_minimum_required(VERSION 3.16)
project(uLayFS)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

add_library(ulayfs SHARED
    src/alloc.h src/alloc.cpp
    src/btable.h
    src/config.h
    src/file.h src/file.cpp
    src/futex.h
    src/layout.h src/layout.cpp
    src/lib.h src/lib.cpp
    src/mtable.h
    src/posix.h
    src/util.h)
target_link_libraries(ulayfs dl)

add_executable(test_basic test/basic.c)
target_link_libraries(test_basic ulayfs)

add_executable(test_futex test/futex.cpp)

add_executable(test_alloc test/alloc.cpp)
target_link_libraries(test_alloc ulayfs)

add_executable(test_util test/util.cpp)
target_link_libraries(test_util ulayfs)
