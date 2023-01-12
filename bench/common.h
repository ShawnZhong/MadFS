#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <climits>
#include <cstring>
#include <thread>
#include <vector>

const char* get_filepath() {
  const char* res = "test.txt";

  if (char* pmem_path = std::getenv("PMEM_PATH"); pmem_path) {
    static char path[PATH_MAX];
    strcpy(path, pmem_path);
    strcat(path, "/test.txt");
    res = path;
  }

  fprintf(stderr, "Benchmark config:\n");
  fprintf(stderr, "\tfilepath: %s\n", res);
  return res;
}

int get_num_iter(int default_val = 10000) noexcept {
  char* str = std::getenv("BENCH_NUM_ITER");
  int num_iter = str ? std::atoi(str) : default_val;
  fprintf(stderr, "\tnum_iter: %d\n", num_iter);
  return num_iter;
}

int get_file_size(int default_val = 1024) noexcept {
  char* str = std::getenv("BENCH_FILE_SIZE");
  int mb = str ? std::atoi(str) : default_val;
  fprintf(stderr, "\tfile_size: %d MB\n", mb);
  return mb * 1024 * 1024;
}

void check_file_size(int fd, size_t file_size) {
  struct stat stat_buf {};
  fstat(fd, &stat_buf);
  if ((size_t)stat_buf.st_size != file_size) {
    fprintf(stderr, "File size is not correct: %ld, expected: %ld\n",
            stat_buf.st_size, file_size);
    throw std::runtime_error("file size mismatch");
  }
}

void prefill_file(int fd, size_t num_bytes,
                  size_t chunk_size = 32l * 1024 * 1024) {
  fprintf(stderr, "prefilling file with %.3f MB in %.3f MB chunk\n",
          num_bytes / 1024. / 1024., chunk_size / 1024. / 1024.);

  auto buf = new char[chunk_size];
  std::fill(buf, buf + chunk_size, 'x');

  for (size_t i = 0; i < num_bytes / chunk_size; ++i) {
    size_t res = write(fd, buf, chunk_size);
    if (res != chunk_size) {
      fprintf(stderr, "write failed at %zu, returned %zu", i * chunk_size, res);
      throw std::runtime_error("write failed");
    }
  }
  if (size_t remaining_size = num_bytes % chunk_size) {
    size_t res = write(fd, buf, remaining_size);
    if (res != remaining_size) {
      fprintf(stderr, "write failed at %zu, returned %zu",
              num_bytes / chunk_size, res);
      throw std::runtime_error("write failed");
    }
  }

  fsync(fd);

  check_file_size(fd, num_bytes);
  delete[] buf;
}

std::vector<size_t> get_cpu_list() {
  std::vector<size_t> res;
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
    size_t num_cpus = std::thread::hardware_concurrency();
    for (size_t i = 0; i < num_cpus; ++i) {
      res.push_back(i);
    }
  }
  return res;
}

void pin_core(size_t thread_index) {
  static std::vector<size_t> cpu_list = get_cpu_list();
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
