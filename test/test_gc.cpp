#include <fcntl.h>

#include "common.h"
#include "gc.h"
#include "lib.h"
#include "logging.h"

const char* filepath = get_filepath();

using ulayfs::BLOCK_SIZE;
using ulayfs::NUM_INLINE_TX_ENTRY;
using ulayfs::NUM_TX_ENTRY_PER_BLOCK;
using ulayfs::debug::clear_count;
using ulayfs::debug::print_file;
using ulayfs::utility::GarbageCollector;

struct BasicTestOpt {
  int num_bytes_per_iter = BLOCK_SIZE;
  int num_iter = NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK + 1;
  bool print = false;

  // if set, the offset will be random in [0, random_block_range * BLOCK_SIZE)
  int random_block_range = 0;
};

void basic_test(BasicTestOpt opt) {
  const auto& [num_bytes_per_iter, num_iter, print, random_block_range] = opt;

  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  auto src_buf = std::make_unique<char[]>(num_bytes_per_iter);

  for (int i = 0; i < num_iter; ++i) {
    ssize_t ret;
    if (random_block_range) {
      off_t offset = (rand() % random_block_range) * BLOCK_SIZE;
      ret = pwrite(fd, src_buf.get(), num_bytes_per_iter, offset);
    } else {
      ret = pwrite(fd, src_buf.get(), num_bytes_per_iter, 0);
    }
    ASSERT(ret == num_bytes_per_iter);
  }
  fsync(fd);

  auto file = ulayfs::get_file(fd);
  auto file_size = file->blk_table.get_file_state_unsafe().file_size;

  if (print) std::cerr << *file;
  close(fd);

  clear_count();

  {
    GarbageCollector garbage_collector(filepath);
    garbage_collector.do_gc();
    if (print) std::cerr << *garbage_collector.get_file();
  }

  {
    int new_fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    auto new_file = ulayfs::get_file(new_fd);
    ASSERT(new_file->blk_table.get_file_state_unsafe().file_size == file_size);
  }
}

void sync_test() {
  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  std::mutex mutex;
  std::condition_variable cv;

  int num_io_threads = 8;
  bool gc_done = false;
  std::vector io_threads_done(num_io_threads, false);
  std::vector<std::thread> io_threads;

  for (int i = 0; i < num_io_threads; ++i) {
    io_threads.emplace_back([&, i]() {
      std::unique_lock<std::mutex> lock{mutex};
      if (i != 0) {
        // wait for the previous thread
        cv.wait(lock, [&]() { return io_threads_done[i - 1]; });
      }

      // the first thread writes NUM_INLINE_TX_ENTRY + 1 blocks
      int num_iter = i == 0 ? NUM_INLINE_TX_ENTRY + 1 : NUM_TX_ENTRY_PER_BLOCK;

      LOG_INFO("thread %d start", i);
      char buf[BLOCK_SIZE]{};
      for (int iter = 0; iter < num_iter; ++iter) {
        auto ret = pwrite(fd, buf, BLOCK_SIZE, 0);
        ASSERT(ret == BLOCK_SIZE);
      }
      fsync(fd);
      LOG_INFO("thread %d finished writing", i);

      // notify the next thread to start
      io_threads_done[i] = true;
      cv.notify_all();

      // keep running until the gc is done
      cv.wait(lock, [&]() { return gc_done; });
      LOG_INFO("thread %d exiting", i);
    });
  }

  std::thread gc([&]() {
    std::unique_lock<std::mutex> lock{mutex};
    cv.wait(lock, [&]() { return io_threads_done[num_io_threads - 1]; });

    LOG_INFO("gc start");

    GarbageCollector garbage_collector(filepath);
    std::cerr << garbage_collector.file->shm_mgr;
    garbage_collector.do_gc();

    LOG_INFO("gc finished");

    gc_done = true;
    cv.notify_all();
  });

  gc.join();
  for (auto& t : io_threads) t.join();
}

int main() {
  srand(0);  // NOLINT(cert-msc51-cpp)

  //  basic_test({1, 1'000'000});
  //  basic_test({BLOCK_SIZE * 64, 1'000'000});

  basic_test({});
  basic_test({.random_block_range = 1000});

  if constexpr (!ulayfs::BuildOptions::use_pmemcheck) {
    // the following tests are too slow for pmemcheck
    basic_test({.num_bytes_per_iter = BLOCK_SIZE * 63});
    basic_test({.num_bytes_per_iter = BLOCK_SIZE * 64});
    basic_test({.num_bytes_per_iter = BLOCK_SIZE * 65});
  }

  sync_test();
}
