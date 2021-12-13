#pragma once

#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

constexpr char FILEPATH[] = "test.txt";
constexpr int MAX_SIZE = 64 * 4096;
constexpr int MAX_NUM_THREAD = 16;
constexpr int NUM_ITER = 10000;

void pin_node(int node) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  if (node == 0) {  // node0: 0-7,16-23
    for (int i = 0; i <= 7; ++i) CPU_SET(i, &cpuset);
    for (int i = 16; i <= 23; ++i) CPU_SET(i, &cpuset);
  } else {  // node1: 8-15,24-31
    for (int i = 8; i <= 15; ++i) CPU_SET(i, &cpuset);
    for (int i = 24; i <= 31; ++i) CPU_SET(i, &cpuset);
  }
  if (sched_setaffinity(getpid(), sizeof(cpuset), &cpuset) == -1) {
    perror("sched_setaffinity");
    exit(EXIT_FAILURE);
  }
}
