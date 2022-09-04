#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstring>

#include "common.h"
#include "const.h"
#include "debug.h"

#define CHECK_RESULT(expected, actual, length, fd)                \
  do {                                                            \
    if (memcmp(expected, actual, length) != 0) {                  \
      ulayfs::debug::print_file(fd);                              \
      std::cerr << "expected: \"";                                \
      for (int i = 0; i < length; ++i) putc(expected[i], stderr); \
      std::cerr << "\"\n";                                        \
      std::cerr << "actual  : \"";                                \
      for (int i = 0; i < length; ++i) putc(actual[i], stderr);   \
      std::cerr << "\"\n";                                        \
      assert(false);                                              \
    }                                                             \
  } while (0)

static void print_file(int fd) {
  if (ulayfs::is_linked()) {
    ulayfs::debug::print_file(fd);
  } else {
    printf("uLayFS not linked\n");
  }
}

static const char* get_filepath() {
  char* pmem_path = std::getenv("PMEM_PATH");
  if (!pmem_path) return "test.txt";
  static char path[PATH_MAX];
  strcpy(path, pmem_path);
  strcat(path, "/test.txt");
  return path;
}

constexpr std::string_view chars =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

static void fill_buff(char* buff, int num_elem, int init = 0) {
  std::generate(buff, buff + num_elem,
                [i = init]() mutable { return chars[i++ % chars.length()]; });
}

static std::string random_string(int length) {
  std::string str;
  str.reserve(length);
  for (int i = 0; i < length; ++i)
    str.push_back(chars[rand() % chars.length()]);
  return str;
}
