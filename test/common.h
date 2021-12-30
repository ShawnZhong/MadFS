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
