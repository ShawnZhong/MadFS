#pragma once

#include <algorithm>
#include <cstdio>

#include "block.h"
#include "debug.h"

#define CHECK_RESULT(expected, actual, length, fd) \
  do {                                             \
    if (memcmp(expected, actual, length) != 0) {   \
      print_file(fd);                              \
      std::cerr << "expected: \"";                 \
      for (char i : expected) putc(i, stderr);     \
      std::cerr << "\"\n";                         \
      std::cerr << "actual  : \"";                 \
      for (char i : actual) putc(i, stderr);       \
      std::cerr << "\"\n";                         \
      assert(false);                               \
    }                                              \
  } while (0)

void print_file(int fd) {
  if (ulayfs::print_file) {
    ulayfs::print_file(fd);
  } else {
    printf("uLayFS not linked\n");
  }
}

constexpr char FILEPATH[] = "test.txt";

char const hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

void fill_buff(char* buff, int num_elem, int init = 0) {
  std::generate(buff, buff + num_elem,
                [i = init]() mutable { return hex_chars[i++ % 16]; });
}
