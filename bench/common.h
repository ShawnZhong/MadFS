#pragma once
#pragma GCC diagnostic ignored "-Wunused-result"

#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

constexpr char FILEPATH[] = "test.txt";

int get_num_iter(int default_val = 10000) noexcept {
  char* num_iter_str = std::getenv("BENCH_NUM_ITER");
  return num_iter_str ? std::atoi(num_iter_str) : default_val;
}
