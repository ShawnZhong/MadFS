/**
 * This microbenchmark aims to measure the tail latency.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <sstream>
#include <vector>

#include "gc.h"

// if necessary, accept these parameters from command-line
constexpr const uint32_t file_size = 1 * 1024 * 1024 * 1024;  // 1GB
constexpr const uint32_t chunk_size = 4096;                   // 4KB
constexpr const uint32_t num_chunks = file_size / chunk_size;
constexpr const char* file_path = "test.txt";
constexpr const uint32_t epoch_sec = 30;
constexpr const uint32_t half_epoch_sec = epoch_sec / 2;
constexpr const uint32_t num_epochs = 1;
constexpr const uint64_t per_epoch_buf_size = 1'000'000 * epoch_sec;
// write throughput is ~2GB/s on the machine we are testing, which means
// 0.5M ops/s, and each op takes ~2us

// example the experiment timeline (30s as a epoch, one epoch):
//   bench process
//   - 0-15s: runs but does not collect latency
//   - 15-45s: runs and collect latency
//   gc process:
//   - @30s start gc (which should clean up garbage generated in the 30s)
// this will ensure the bench process' latency measurement will capture the
// garbage collection window

bool has_gc = false;  // whether this experiment has a gc daemon
bool is_gc = false;   // whether the current process is a gc daemon

void prep() {
  unlink(file_path);  // make sure the file does not exist

  int fd = open(file_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    std::cerr << "Fail to prepare the file: open error" << std::endl;
    exit(1);
  }

  auto buf = new char[chunk_size];
  std::fill(buf, buf + chunk_size, 'y');
  for (uint32_t i = 0; i < num_chunks; ++i) {
    ssize_t ret = write(fd, buf, chunk_size);
    if (ret != chunk_size) {
      std::cerr << "Fail to prepare the file: write error (ret=" << ret
                << ", offset=" << i * chunk_size << ", size=" << chunk_size
                << ")" << std::endl;
      exit(1);
    }
  }
  delete[] buf;

  struct stat stat_buf;
  fstat(fd, &stat_buf);
  if (stat_buf.st_size != file_size) {
    std::cerr << "Incorrect file size: expect=" << file_size
              << ", actual=" << stat_buf.st_size << std::endl;
  }

  close(fd);
}

void run_bench() {
  std::vector<std::vector<uint32_t>> latency_bufs(num_epochs);
  for (auto& b : latency_bufs) b.reserve(per_epoch_buf_size);

  auto src_buf = new char[chunk_size];
  std::fill(src_buf, src_buf + chunk_size, 'z');

  int fd = open(file_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    std::cerr << "Fail to run benchmark: open error" << std::endl;
    exit(1);
  }

  // first run half of epochs without measurement to heat up
  auto t0 = std::chrono::high_resolution_clock::now();
  do {
    int64_t offset =
        (static_cast<uint64_t>(std::rand()) % num_chunks) * chunk_size;
    ssize_t ret = pwrite(fd, src_buf, chunk_size, offset);
    if (ret != chunk_size) {
      std::cerr << "Fail to pwrite to the file: ret=" << ret
                << ", offset=" << offset << ", size=" << chunk_size << ")"
                << std::endl;
      exit(1);
    }
  } while (duration_cast<std::chrono::microseconds>(
               std::chrono::high_resolution_clock::now() - t0)
               .count());
  auto t1 = std::chrono::high_resolution_clock::now();

  while (true) {
    int64_t offset =
        (static_cast<uint64_t>(std::rand()) % num_chunks) * chunk_size;
    auto t2 = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] ssize_t ret = pwrite(fd, src_buf, chunk_size, offset);
    auto t3 = std::chrono::high_resolution_clock::now();
    assert(ret == chunk_size);
    uint32_t latency =
        duration_cast<std::chrono::microseconds>(t3 - t2).count();
    uint32_t e =
        duration_cast<std::chrono::seconds>(t3 - t1).count() / epoch_sec;
    if (e >= num_epochs) break;
    latency_bufs[e].emplace_back(latency);
  }

  for (auto& b : latency_bufs) {
    if (b.size() > per_epoch_buf_size) {
      std::cerr << "Warning: a epoch has more ops dones than the reserved "
                   "buffer slots: reserved="
                << per_epoch_buf_size << ", actual=" << b.size() << std::endl;
    }
  }

  std::stringstream ss;
  for (size_t i = 0; i < num_epochs; ++i) {
    std::sort(latency_bufs[i].begin(), latency_bufs[i].end(),
              std::greater<uint32_t>());
    ss << "epoch-" << i << ": ";

    if (latency_bufs.size() / 10 == 0) continue;
    ss << "p90=" << latency_bufs[i][latency_bufs.size() / 10] << ", ";

    uint32_t scale = 100;
    uint32_t idx;
    while (true) {
      idx = latency_bufs.size() / scale;
      if (idx < 100) break;
      ss << ", p" << scale - 1 << "=" << latency_bufs[i][idx];
      scale *= 10;
    }
    ss << std::endl;
  }

  std::cout << ss.str();
}

void run_gc() {
  ulayfs::utility::GarbageCollector garbage_collector(file_path);
  std::vector<int> gc_duration;
  gc_duration.reserve(10);
  for (uint32_t e = 0; e < num_epochs; ++e) {
    std::this_thread::sleep_for(std::chrono::seconds(epoch_sec));
    auto t0 = std::chrono::high_resolution_clock::now();
    bool is_done = garbage_collector.do_gc();
    auto t1 = std::chrono::high_resolution_clock::now();
    gc_duration.emplace_back(
        duration_cast<std::chrono::microseconds>(t1 - t0).count());
    if (!is_done) std::cerr << "Warning: No GC is done!" << std::endl;
  }
  // make sure the output is not interleaving
  std::stringstream ss;
  for (size_t i = 0; i < num_epochs; ++i)
    ss << "epoch-" << i << ": " << gc_duration[i] << " us\n";
  std::cout << ss.str();
}

int main(int argc, char** argv) {
  if (argc > 2) goto usage;
  if (argc == 2) {
    if (strcmp(argv[1], "--prep") == 0) {
      prep();
      return 0;
    }
    if (strcmp(argv[1], "--gc") == 0) {
      has_gc = true;
      int pid = fork();
      if (pid < 0) {
        std::cerr << "Fail to fork a garbage collection daemon!" << std::endl;
        exit(1);
      }
      is_gc = (pid == 0);
    }
  }

  if (is_gc)
    run_gc();
  else
    run_bench();

  return 0;

usage:
  std::cerr << "usage: " << argv[0] << " [--prep] [--gc]" << std::endl
            << "  --prep: only prepare the files" << std::endl
            << "  --gc:   fork a garbage collection daemon" << std::endl;
  return 1;
}
