cmake_minimum_required(VERSION 3.16)
project(uLayFS)

set(CMAKE_CXX_STANDARD 20)

add_library(ulayfs SHARED src/config.h src/file.cpp src/file.h src/futex.h src/layout.h src/lib.cpp src/posix.h)
target_link_libraries(ulayfs dl)

add_executable(test_basic test/basic.c)
target_link_libraries(test_basic ulayfs)

add_executable(test_futex test/futex.cpp)
