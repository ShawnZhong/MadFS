#include <fcntl.h>

#include "common.h"
#include "gc.h"
#include "logging.h"

const char* filepath = get_filepath();

using ulayfs::BLOCK_SIZE;
using ulayfs::NUM_INLINE_TX_ENTRY;
using ulayfs::NUM_TX_ENTRY_PER_BLOCK;
using ulayfs::debug::print_file;
using ulayfs::utility::GarbageCollector;

struct TestOpt {
  int num_bytes_per_iter = BLOCK_SIZE;
  int num_iter = NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK + 1;
  bool print = false;
};

void test(TestOpt test_opt) {
  const auto& [num_bytes_per_iter, num_iter, print] = test_opt;

  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  auto src_buf = std::make_unique<char[]>(num_bytes_per_iter);

  for (int i = 0; i < num_iter; ++i) {
    ssize_t ret = pwrite(fd, src_buf.get(), num_bytes_per_iter, 0);
    ASSERT(ret == num_bytes_per_iter);
  }
  fsync(fd);
  if (print) print_file(fd);

  close(fd);

  GarbageCollector garbage_collector(filepath);
  garbage_collector.gc();
  if (print) std::cerr << *garbage_collector.get_file();
}

int main() {
  test({BLOCK_SIZE, 1});
  test({BLOCK_SIZE, NUM_INLINE_TX_ENTRY + 1});
  test({BLOCK_SIZE, NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK});
  test({BLOCK_SIZE, NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK * 3 + 1});
  if constexpr (!ulayfs::BuildOptions::use_pmemcheck) {
    // the following tests are too slow for pmemcheck
    test({BLOCK_SIZE * 63});
    test({BLOCK_SIZE * 64});
    test({BLOCK_SIZE * 65});
  }
}
