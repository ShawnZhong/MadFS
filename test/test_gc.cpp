#include <fcntl.h>

#include "common.h"
#include "gc.h"
#include "logging.h"

#define ASSERT(x) PANIC_IF(!(x), "assertion failed: " #x)

const char* filepath = get_filepath();

static constexpr int SIZE_PER_ITER = ulayfs::BLOCK_SIZE * 2;
static constexpr int NUM_ITER =
    ulayfs::NUM_INLINE_TX_ENTRY + ulayfs::NUM_TX_ENTRY_PER_BLOCK * 4 + 3;

int main() {
  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char buff[SIZE_PER_ITER]{};

  for (int i = 0; i < NUM_ITER; ++i) {
    ssize_t ret = pwrite(fd, buff, SIZE_PER_ITER, 0);
    ASSERT(ret == SIZE_PER_ITER);
  }
  fsync(fd);

  ulayfs::debug::print_file(fd);
  close(fd);

  ulayfs::utility::GarbageCollector garbage_collector(filepath);
  garbage_collector.gc();
  std::cerr << *garbage_collector.get_file();
}
