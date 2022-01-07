#pragma once
#pragma GCC diagnostic ignored "-Wunused-result"

#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

constexpr char FILEPATH[] = "test.txt";
constexpr int MAX_SIZE = 128 * 1024;
constexpr int MAX_NUM_THREAD = 16;

int fd;
int num_iter = []() noexcept {
  auto str = std::getenv("BENCH_NUM_ITER");
  if (str) return std::stoi(str);
  return 10000;
}();
