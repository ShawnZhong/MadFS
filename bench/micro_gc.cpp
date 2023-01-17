/**
 * This microbenchmark aims to measure the tail latency.
 * To run this benchmark, first prepare the file (default 1GB) and run with and
 * without garbage collection:
 *  ./build-release/micro_tail --gc --io -f /mnt/pmem0-ext4-dax/test.txt
 *  ./build-release/micro_tail --io -f /mnt/pmem0-ext4-dax/test.txt
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cxxopts.hpp>
#include <filesystem>
#include <iostream>
#include <vector>

#include "common.h"
#include "gc.h"

// if necessary, accept these parameters from command-line
constexpr const uint32_t file_size = 1 * 1024 * 1024 * 1024;  // 1GB
constexpr const uint32_t chunk_size = 4096;                   // 4KB
constexpr const uint32_t num_chunks = file_size / chunk_size;

// write throughput is ~2GB/s on the machine we are testing, which means
// 0.5M ops/s, and each op takes ~2us. we thus reserve 1M slot per second for
// recording latency
constexpr const uint32_t epoch_sec = 30;
constexpr const uint32_t pre_heat_sec = epoch_sec / 2;
constexpr const uint32_t num_epochs = 2;
constexpr const uint64_t per_epoch_buf_size = 1'000'000 * epoch_sec;

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

void prep(const char* file_path) {
  unlink(file_path);  // make sure the file does not exist

  int fd = open(file_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    std::cerr << "Fail to open file during prepare" << file_path << std::endl;
    exit(1);
  }

  prefill_file(fd, file_size, chunk_size);

  close(fd);
  std::cerr << "File preparation done: size=" << file_size << std::endl;
}

void export_result(const std::filesystem::path& output_dir,
                   const std::vector<std::vector<uint32_t>>& latency_bufs) {
  std::cerr << "Exporting results..." << std::endl;

  std::filesystem::create_directories(output_dir);

  for (size_t epoch = 0; epoch < latency_bufs.size(); epoch++) {
    auto path = output_dir / ("epoch-" + std::to_string(epoch));
    int fd = madfs::posix::open(path.c_str(), O_CREAT | O_RDWR | O_APPEND,
                                 S_IRUSR | S_IWUSR);
    if (fd < 0) {
      std::cerr << "Fail to export result: open error" << std::endl;
      throw std::runtime_error("Fail to export result: open error");
    }

    const auto& latency_buf = latency_bufs[epoch];
    auto len = latency_buf.size() * sizeof(uint32_t);
    auto ret = madfs::posix::write(fd, latency_buf.data(), len);

    if (ret != len) {
      std::cerr << "Fail to export result: write error. ret=" << ret
                << ", len=" << len << std::endl;
      throw std::runtime_error("Fail to export result: write error");
    }

    madfs::posix::close(fd);
  }

  std::cerr << "Benchmark result exported to " << output_dir << std::endl;
}

void run_io(const char* file_path, const std::filesystem::path& output_dir) {
  std::vector<std::vector<uint32_t>> latency_bufs(num_epochs);
  for (auto& b : latency_bufs) b.reserve(per_epoch_buf_size);

  auto src_buf = new char[chunk_size];
  std::fill(src_buf, src_buf + chunk_size, 'z');

  int fd = open(file_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    std::cerr << "Fail to run benchmark: open error" << std::endl;
    exit(1);
  }

  check_file_size(fd, file_size);

  // first run half of epochs without measurement to heat up
  {
    auto begin = std::chrono::high_resolution_clock::now();
    do {
      int64_t offset =
          (static_cast<uint64_t>(std::rand()) % num_chunks) * chunk_size;
      ssize_t ret = pwrite(fd, src_buf, chunk_size, offset);
      assert(ret == chunk_size);
      fsync(fd);
    } while (duration_cast<std::chrono::seconds>(
                 std::chrono::high_resolution_clock::now() - begin)
                 .count() < pre_heat_sec);
  }

  {
    auto exp_begin = std::chrono::high_resolution_clock::now();
    while (true) {
      int64_t offset =
          (static_cast<uint64_t>(std::rand()) % num_chunks) * chunk_size;
      auto op_begin = std::chrono::high_resolution_clock::now();
      [[maybe_unused]] ssize_t ret = pwrite(fd, src_buf, chunk_size, offset);
      assert(ret == chunk_size);
      fsync(fd);
      auto op_end = std::chrono::high_resolution_clock::now();
      uint32_t latency =
          duration_cast<std::chrono::nanoseconds>(op_end - op_begin).count();
      uint32_t e =
          duration_cast<std::chrono::seconds>(op_end - exp_begin).count() /
          epoch_sec;
      if (e >= num_epochs) break;
      latency_bufs[e].emplace_back(latency);
    }
  }

  close(fd);
  std::cerr << "Finish benchmark...\n";

  for (auto& b : latency_bufs) {
    if (b.size() > per_epoch_buf_size) {
      std::cerr << "Warning: a epoch has more ops dones than the reserved "
                   "buffer slots: reserved="
                << per_epoch_buf_size << ", actual=" << b.size() << std::endl;
    }
  }

  export_result(output_dir, latency_bufs);
}

void run_gc(const char* file_path) {
  madfs::utility::GarbageCollector garbage_collector(file_path);
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

struct Args {
  bool run_gc = false;
  bool run_io = false;
  bool skip_prep = false;
  std::filesystem::path file_path = "test.txt";
  std::filesystem::path output_path = "output";

  static Args parse(int argc, char** argv) {
    cxxopts::Options options("tail_latency", "Tail latency benchmark");
    Args args;
    options.add_options(
        {}, {
                {"f,file", "Path to the file to store data",
                 cxxopts::value<std::filesystem::path>(args.file_path)},
                {"o,output", "Result output path",
                 cxxopts::value<std::filesystem::path>(args.output_path)},
                {"gc", "Run gc thread", cxxopts::value<bool>(args.run_gc)},
                {"io", "Run IO thread", cxxopts::value<bool>(args.run_io)},
                {"no-prep", "Skip file preparation",
                 cxxopts::value<bool>(args.skip_prep)},
                {"help", "Print help"},
            });

    auto result = options.parse(argc, argv);
    if (result.count("help") || (!args.run_gc && !args.run_io)) {
      std::cout << options.help() << std::endl;
      exit(0);
    }
    return args;
  }

  friend std::ostream& operator<<(std::ostream& os, const Args& args) {
    os << "run_gc: " << args.run_gc << " run_io: " << args.run_io
       << " skip_prep: " << args.skip_prep
       << " file_path: " << absolute(args.file_path)
       << " output_path: " << absolute(args.output_path);
    return os;
  }
};

int main(int argc, char** argv) {
  const auto args = Args::parse(argc, argv);
  std::cerr << args << std::endl;

  if (!args.skip_prep) prep(args.file_path.c_str());

  std::thread gc_thread;
  if (args.run_gc) gc_thread = std::thread(run_gc, args.file_path.c_str());

  std::thread io_thread;
  if (args.run_io)
    io_thread = std::thread(run_io, args.file_path.c_str(), args.output_path);

  if (io_thread.joinable()) io_thread.join();
  if (gc_thread.joinable()) gc_thread.join();

  return 0;
}
