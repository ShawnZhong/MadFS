#include <fcntl.h>

#include <cassert>
#include <iostream>

#include "common.h"
#include "layout.h"
#include "lib.h"

using namespace ulayfs;

constexpr auto NUM_BYTES = 64;
constexpr auto OFFSET = 4096 * 2 + 1234;
constexpr auto NUM_ITER = (NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY) / 2 + 1;

int main(int argc, char* argv[]) {
  [[maybe_unused]] ssize_t ret;

  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  auto file = files[fd];

  lseek(fd, OFFSET, SEEK_SET);

  for (int i = 0; i < NUM_ITER; ++i) {
    char src_buf[NUM_BYTES];
    fill_buff(src_buf, NUM_BYTES, NUM_BYTES * i);
    ret = write(fd, src_buf, NUM_BYTES);
    assert(ret == NUM_BYTES);
  }

  std::cerr << *file << "\n";

  // check that the content before OFFSET are all zeros
  lseek(fd, 0, SEEK_SET);
  {
    char actual[OFFSET]{};
    char expected[OFFSET]{};
    ret = read(fd, actual, OFFSET);
    assert(ret == OFFSET);
    assert(memcmp(expected, actual, OFFSET));
  }

  // check that content after OFFSET are written
  {
    constexpr int length = NUM_BYTES * NUM_ITER;

    char actual[length]{};
    ret = read(fd, actual, length);
    assert(ret == length);

    char expected[length]{};
    fill_buff(expected, length);

    assert(memcmp(expected, actual, length) == 0);
  }
}
