#pragma once

#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <numa.h>  // sudo apt install libnuma-dev
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

void append_file(int fd, long num_bytes, int num_iter = 1) {
  auto buf = new char[num_bytes];
  std::fill(buf, buf + num_bytes, 'x');
  for (int i = 0; i < num_iter; ++i) {
    [[maybe_unused]] ssize_t res = write(fd, buf, num_bytes);
    assert(res == num_bytes);
  }
  fsync(fd);
  delete[] buf;
}

bool is_ulayfs_linked() { return ulayfs::debug::print_file != nullptr; }

std::vector<int> get_cpu_list(int target_node) {
  std::vector<int> res;
  int num_cpus = std::thread::hardware_concurrency();
  for (int cpu = 0; cpu < num_cpus; ++cpu) {
    if (numa_node_of_cpu(cpu) == target_node) {
      res.push_back(cpu);
    }
  }
  return res;
}

std::vector<int> get_cpu_list() {
  int cpu = sched_getcpu();
  int node = numa_node_of_cpu(cpu);
  return get_cpu_list(node);
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
