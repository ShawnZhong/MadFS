#include <fcntl.h>

#include <cassert>
#include <iostream>

#include "common.h"

template <int NUM_BYTES, int NUM_ITER = 1, int OFFSET = 0>
int test_rw() {
  [[maybe_unused]] ssize_t ret;

  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  auto file = get_file(fd);

  lseek(fd, OFFSET, SEEK_SET);

  for (int i = 0; i < NUM_ITER; ++i) {
    char src_buf[NUM_BYTES];
    fill_buff(src_buf, NUM_BYTES, NUM_BYTES * i);
    ret = write(fd, src_buf, NUM_BYTES);
    assert(ret == NUM_BYTES);
  }

  // check that the content before OFFSET are all zeros
  lseek(fd, 0, SEEK_SET);
  if (OFFSET != 0) {
    char actual[OFFSET]{};
    char expected[OFFSET]{};
    ret = read(fd, actual, OFFSET);
    assert(ret == OFFSET);
    CHECK_RESULT(expected, actual, OFFSET, file);
  }

  // check that content after OFFSET are written
  {
    constexpr int length = NUM_BYTES * NUM_ITER;

    char actual[length]{};
    ret = read(fd, actual, length);
    assert(ret == length);

    char expected[length]{};
    fill_buff(expected, length);

    CHECK_RESULT(expected, actual, length, file);
  }

  return fd;
}

int main(int argc, char* argv[]) {
  // everything block-aligned
  test_rw<BLOCK_SIZE>();
  test_rw<BLOCK_SIZE * 8>();
  test_rw<BLOCK_SIZE, 2>();
  test_rw<BLOCK_SIZE * 8, 2>();

  // single-block write w/ block-aligned starting offset
  test_rw<8>();
  test_rw<64>();
  test_rw<BLOCK_SIZE / 2>();
  test_rw<BLOCK_SIZE - 1>();

  // single-block write w/o alignment
  test_rw<8, 1, 8>();
  test_rw<BLOCK_SIZE - 8, 1, 8>();
  test_rw<8, 1, BLOCK_SIZE - 8>();
  test_rw<BLOCK_SIZE / 2, 1, BLOCK_SIZE / 2>();
  test_rw<8, BLOCK_SIZE / 8>();
  test_rw<42, 13, 123>();

  // multi-block write w/ block-aligned starting offset
  test_rw<BLOCK_SIZE + 1>();
  test_rw<BLOCK_SIZE * 2 - 1>();
  test_rw<BLOCK_SIZE * 16 + 1>();
  test_rw<BLOCK_SIZE + 1, 1, BLOCK_SIZE * 2>();
  test_rw<12345, 1, BLOCK_SIZE * 7>();

  // multi-block write w/o alignment
  test_rw<BLOCK_SIZE, 1, 8>();
  test_rw<BLOCK_SIZE * 16 + 1, 1, BLOCK_SIZE - 1>();
  test_rw<BLOCK_SIZE * 16 - 13, 1, 13>();
  test_rw<12345, 6, 7890>();

  return 0;
}
