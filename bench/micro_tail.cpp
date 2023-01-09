/**
 * This microbenchmark aims to measure the tail latency.
 * To run this benchmark, first prepare the file (default 1GB) and run without
 * garbage collection:
 *   LD_PRELOAD=./build-release/libulayfs.so  ./build-release/micro_tail --prep
 *   LD_PRELOAD=./build-release/libulayfs.so  ./build-release/micro_tail
 *
 * Then re-prepare the file and re-run with garbage collection (re-preparation
 * is necessary because previous run will modify the file):
 *   LD_PRELOAD=./build-release/libulayfs.so  ./build-release/micro_tail --prep
 *   LD_PRELOAD=./build-release/libulayfs.so  ./build-release/micro_tail --gc
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
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

// write throughput is ~2GB/s on the machine we are testing, which means
// 0.5M ops/s, and each op takes ~2us. we thus reserve 1M slot per second for
// recording latency
constexpr const uint32_t epoch_sec = 30;
constexpr const uint32_t half_epoch_sec = epoch_sec / 2;
constexpr const uint32_t num_epochs = 2;
constexpr const uint64_t per_epoch_buf_size = 1'000'000 * epoch_sec;

// we require enough datapoint when calculating tail latency
constexpr const uint32_t min_num_datapoints = 1000;

// the experiment consists of multiple epochs. the bench process records latency
// in the window of [(e+1/2)*epoch, e+3/2*epoch); the gc process performs clean
// up at (e*epoch), for e in range(num_epochs)
//
// example the experiment timeline (30s as a epoch, one epoch):
//   bench process
//   - 0-15s: runs but does not collect latency
//   - 15-45s: runs and collect latency
//   gc process:
//   - @30s start gc (which should clean up garbage generated in the 30s)
// this will ensure the bench process' latency measurement will capture the
// garbage collection window

int gc_pid = -1;  // 0 means the current process is gc process; -1 means no gc

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
    exit(1);
  }

  close(fd);
  std::cerr << "File preparation done: size=" << file_size << std::endl;
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

  struct stat stat_buf;
  fstat(fd, &stat_buf);
  if (stat_buf.st_size != file_size) {
    std::cerr << "Incorrect file size: expect=" << file_size
              << ", actual=" << stat_buf.st_size << std::endl;
    exit(1);
  }

  // first run half of epochs without measurement to heat up
  auto t0 = std::chrono::high_resolution_clock::now();
  do {
    int64_t offset =
        (static_cast<uint64_t>(std::rand()) % num_chunks) * chunk_size;
    ssize_t ret = pwrite(fd, src_buf, chunk_size, offset);
    assert(ret == chunk_size);
    fsync(fd);
  } while (duration_cast<std::chrono::seconds>(
               std::chrono::high_resolution_clock::now() - t0)
               .count() < half_epoch_sec);

  auto t1 = std::chrono::high_resolution_clock::now();
  while (true) {
    int64_t offset =
        (static_cast<uint64_t>(std::rand()) % num_chunks) * chunk_size;
    auto t2 = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] ssize_t ret = pwrite(fd, src_buf, chunk_size, offset);
    assert(ret == chunk_size);
    fsync(fd);
    auto t3 = std::chrono::high_resolution_clock::now();
    uint32_t latency = duration_cast<std::chrono::nanoseconds>(t3 - t2).count();
    uint32_t e =
        duration_cast<std::chrono::seconds>(t3 - t1).count() / epoch_sec;
    if (e >= num_epochs) break;
    latency_bufs[e].emplace_back(latency);
  }

  close(fd);
  std::cerr << "Finish benchmark...\n";  // sorting below may take long...

  for (auto& b : latency_bufs) {
    if (b.size() > per_epoch_buf_size) {
      std::cerr << "Warning: a epoch has more ops dones than the reserved "
                   "buffer slots: reserved="
                << per_epoch_buf_size << ", actual=" << b.size() << std::endl;
    }
  }

  std::stringstream ss;
  ss << "Tail latency (nanosecond):\n";
  for (size_t e = 0; e < num_epochs; ++e) {
    auto& latency_buf = latency_bufs[e];
    std::sort(latency_buf.begin(), latency_buf.end(), std::greater<uint32_t>());
    ss << "epoch-" << e << ": ";

    if (latency_buf.size() / 10 == 0) {
      ss << std::endl;
      continue;
    }
    ss << "p50=" << latency_buf[latency_buf.size() / 2]
       << ", p90=" << latency_buf[latency_buf.size() / 10];

    uint32_t scale = 100;
    uint32_t idx;
    while (true) {
      idx = latency_buf.size() / scale;
      if (idx < min_num_datapoints) break;  // too few datapoints
      ss << ", p" << scale - 1 << "=" << latency_buf[idx];
      scale *= 10;
    }
    ss << std::endl;
  }

  std::cout << ss.str();
}

void run_gc() {
  ulayfs::utility::GarbageCollector garbage_collector(file_path);
  std::vector<uint32_t> gc_duration;
  gc_duration.reserve(num_epochs);
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
  ss << "GC duration (microsecond):\n";
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
      gc_pid = fork();
      if (gc_pid < 0) {
        std::cerr << "Fail to fork a garbage collection daemon!" << std::endl;
        exit(1);
      }
    }
  }

  if (gc_pid == 0)
    run_gc();
  else
    run_bench();

  if (gc_pid > 0) {
    int gc_stat;
    waitpid(gc_pid, &gc_stat, 0);
    if (!WIFEXITED(gc_stat)) {
      std::cerr << "GC daemon exits unexpectedly";
      if (WIFSIGNALED(gc_stat))
        std::cerr << ": Killed by " << strsignal(WTERMSIG(gc_stat));
      std::cerr << std::endl;
      exit(1);
    }
  }

  return 0;

usage:
  std::cerr << "usage: " << argv[0] << " [--prep] [--gc]" << std::endl
            << "  --prep: only prepare the files" << std::endl
            << "  --gc:   fork a garbage collection daemon" << std::endl;
  return 1;
}
