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

struct TestOpt {
  int num_bytes_per_iter = BLOCK_SIZE;
  int num_iter = NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK + 1;
  bool print = false;

  // if set, the offset will be random in [0, random_block_range * BLOCK_SIZE)
  int random_block_range = 0;
};

void test(TestOpt test_opt) {
  const auto& [num_bytes_per_iter, num_iter, print, random_block_range] =
      test_opt;

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
    garbage_collector.gc();
    if (print) std::cerr << *garbage_collector.get_file();
  }

  {
    int new_fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    auto new_file = ulayfs::get_file(new_fd);
    ASSERT(new_file->blk_table.get_file_state().file_size == file_size);
  }
}

int main() {
  srand(0);  // NOLINT(cert-msc51-cpp)

  //  test({1, 1'000'000});
  //  test({BLOCK_SIZE * 64, 1'000'000});
  test({.num_bytes_per_iter = BLOCK_SIZE,
        .num_iter = NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK + 1,
        .print = true,
        .random_block_range = 1000});
  test({BLOCK_SIZE});
  if constexpr (!ulayfs::BuildOptions::use_pmemcheck) {
    // the following tests are too slow for pmemcheck
    test({BLOCK_SIZE * 63});
    test({BLOCK_SIZE * 64});
    test({BLOCK_SIZE * 65});
  }
}
