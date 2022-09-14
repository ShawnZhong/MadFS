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
  auto file_size = file->blk_table.get_file_state().file_size;

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
    ASSERT(new_file->blk_table.get_file_state().file_size == file_size);
  }
}

/**
 * Test the basic functionality of garbage collection.
 *
 * Launch a number of io threads to write to the file, and then run the gc
 * thread while the io threads are in the background.
 */
void sync_test() {
  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  int num_io_threads = 4;
  std::atomic<bool> gc_done = false;
  std::vector<std::atomic<bool>> io_threads_done(num_io_threads);
  std::vector<std::thread> io_threads;

  for (int i = 0; i < num_io_threads; ++i) {
    io_threads.emplace_back([&, i]() {
      if (i != 0) {
        // wait for the previous thread to finish
        while (!io_threads_done[i - 1].load()) {
          std::this_thread::yield();
        }
      }

      int num_iter = i == 0
                         ? NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK * 5 + 1
                         : NUM_TX_ENTRY_PER_BLOCK;

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

      // keep running until the gc is done
      while (!gc_done.load()) {
        std::this_thread::yield();
      }
      LOG_INFO("thread %d exiting", i);
    });
  }

  std::thread gc_thread([&]() {
    while (!io_threads_done[num_io_threads - 1].load()) {
      std::this_thread::yield();
    }
    LOG_INFO("gc start");

    GarbageCollector garbage_collector(filepath);
    std::cerr << garbage_collector.file->tx_mgr;
    std::cerr << garbage_collector.file->shm_mgr;
    garbage_collector.do_gc();
    std::cerr << garbage_collector.file->tx_mgr;

    LOG_INFO("gc finished");

    gc_done = true;
  });

  gc_thread.join();
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
