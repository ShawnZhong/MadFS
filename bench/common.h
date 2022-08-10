#pragma once

#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include <climits>
#include <cstring>
#include <thread>

#include "debug.h"

const char* filepath = []() -> const char* {
  const char* res = "test.txt";

  char* pmem_path = std::getenv("PMEM_PATH");
  if (pmem_path) {
    static char path[PATH_MAX];
    strcpy(path, pmem_path);
    strcat(path, "/test.txt");
    res = path;
  }

  fprintf(stderr, "================ filepath: %s ================ \n", res);
  return res;
}();

int get_num_iter(int default_val = 10000) noexcept {
  char* num_iter_str = std::getenv("BENCH_NUM_ITER");
  int num_iter = num_iter_str ? std::atoi(num_iter_str) : default_val;
  fprintf(stderr, "================ num_iter: %d =============== \n", num_iter);
  return num_iter;
}

void prefill_file(int fd, size_t num_bytes,
                  size_t chunk_size = 32l * 1024 * 1024) {
  auto buf = new char[chunk_size];
  std::fill(buf, buf + chunk_size, 'x');

  for (size_t i = 0; i < num_bytes / chunk_size; ++i) {
    [[maybe_unused]] size_t res = write(fd, buf, chunk_size);
    assert(res == chunk_size);
  }
  if (size_t remaining_size = num_bytes % chunk_size) {
    [[maybe_unused]] size_t res = write(fd, buf, remaining_size);
    assert(res == remaining_size);
  }

  fsync(fd);
  delete[] buf;
}

bool is_ulayfs_linked() { return ulayfs::debug::print_file != nullptr; }

std::vector<int> get_cpu_list() {
  std::vector<int> res;
  char* cpu_list_str = std::getenv("CPULIST");
  if (cpu_list_str) {
    char* p = strtok(cpu_list_str, ",");
    while (p) {
      res.push_back(std::atoi(p));
      p = strtok(nullptr, ",");
    }
  } else {
    fprintf(stderr,
            "environment variable CPULIST not set. "
            "Thread i is pinned to core i\n");
    int num_cpus = std::thread::hardware_concurrency();
    for (int i = 0; i < num_cpus; ++i) {
      res.push_back(i);
    }
  }
  return res;
}

void pin_core(size_t thread_index) {
  static std::vector<int> cpu_list = get_cpu_list();
  if (thread_index >= cpu_list.size()) {
    fprintf(stderr, "thread_index: %ld is out of range\n", thread_index);
    return;
  }

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_list[thread_index], &cpuset);
  if (sched_setaffinity(0, sizeof(cpuset), &cpuset) == -1) {
    fprintf(stderr, "sched_setaffinity failed for thread_index %ld.\n",
            thread_index);
  }
}
