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
#include "utils/logging.h"

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

#define ASSERT(x) PANIC_IF(!(x), "assertion failed: " #x)

static const char* get_filepath() {
  const char* res = "test.txt";

  if (char* pmem_path = std::getenv("PMEM_PATH"); pmem_path) {
    static char path[PATH_MAX];
    strcpy(path, pmem_path);
    strcat(path, "/test.txt");
    res = path;
  }

  fprintf(stderr, "filepath: %s\n", res);
  return res;
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
