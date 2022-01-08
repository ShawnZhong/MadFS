#pragma once

#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include <climits>
#include <cstring>

const char* filepath = []() -> const char* {
  char* pmem_path = std::getenv("PMEM_PATH");
  if (!pmem_path) return "test.txt";
  static char path[PATH_MAX];
  strcpy(path, pmem_path);
  strcat(path, "/test.txt");
  return path;
}();

int get_num_iter(int default_val = 10000) noexcept {
  char* num_iter_str = std::getenv("BENCH_NUM_ITER");
  return num_iter_str ? std::atoi(num_iter_str) : default_val;
}
