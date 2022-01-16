#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstring>

#include "const.h"
#include "debug.h"

#define CHECK_RESULT(expected, actual, length, fd)                \
  do {                                                            \
    if (memcmp(expected, actual, length) != 0) {                  \
      print_file(fd);                                             \
      std::cerr << "expected: \"";                                \
      for (int i = 0; i < length; ++i) putc(expected[i], stderr); \
      std::cerr << "\"\n";                                        \
      std::cerr << "actual  : \"";                                \
      for (int i = 0; i < length; ++i) putc(actual[i], stderr);   \
      std::cerr << "\"\n";                                        \
      assert(false);                                              \
    }                                                             \
  } while (0)

void print_file(int fd) {
  if (ulayfs::print_file) {
    ulayfs::print_file(fd);
  } else {
    printf("uLayFS not linked\n");
  }
}

const char* filepath = []() -> const char* {
  char* pmem_path = std::getenv("PMEM_PATH");
  if (!pmem_path) return "test.txt";
  static char path[PATH_MAX];
  strcpy(path, pmem_path);
  strcat(path, "/test.txt");
  return path;
}();

constexpr std::string_view chars =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

void fill_buff(char* buff, int num_elem, int init = 0) {
  std::generate(buff, buff + num_elem,
                [i = init]() mutable { return chars[i++ % chars.length()]; });
}

std::string random_string(int length) {
  std::string str;
  str.reserve(length);
  for (int i = 0; i < length; ++i)
    str.push_back(chars[rand() % chars.length()]);
  return str;
}
