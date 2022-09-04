#include <fcntl.h>

#include "common.h"
#include "gc.h"
#include "lib.h"
#include "logging.h"

// use ASSERT for non debug build as well
#define ASSERT(x) PANIC_IF(!(x), "assertion failed: " #x)

const char* filepath = get_filepath();

static constexpr int SIZE_PER_ITER = 4096;
static constexpr int NUM_ITER = 1000;

int main() {
  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char buff[SIZE_PER_ITER];

  for (int i = 0; i < NUM_ITER; ++i) {
    ssize_t ret = pwrite(fd, buff, SIZE_PER_ITER, 0);
    ASSERT(ret == SIZE_PER_ITER);
  }

  ulayfs::debug::print_file(fd);

  auto file = ulayfs::get_file(fd);
  ulayfs::utility::GarbageCollector garbage_collector(file.get());

  garbage_collector.gc();

  ulayfs::debug::print_file(fd);
}