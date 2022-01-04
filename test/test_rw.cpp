#include <fcntl.h>

#include <cassert>
#include <iostream>

#include "common.h"

using ulayfs::BLOCK_SIZE;
using ulayfs::NUM_INLINE_TX_ENTRY;
using ulayfs::NUM_TX_ENTRY;

struct TestOpt {
  int num_bytes_per_iter = BLOCK_SIZE;
  int num_iter = 1;
  int init_offset = 0;
};

void test(TestOpt test_opt) {
  const auto& [num_bytes_per_iter, num_iter, init_offset] = test_opt;

  fprintf(stderr,
          "\n\n\n====== "
          "num_bytes_per_iter = %d, "
          "num_iter = %d, "
          "init_offset = %d "
          "======\n",
          num_bytes_per_iter, num_iter, init_offset);

  [[maybe_unused]] ssize_t ret;

  // write data
  {
    unlink(FILEPATH);
    int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, init_offset, SEEK_SET);

    for (int i = 0; i < num_iter; ++i) {
      char src_buf[num_bytes_per_iter];
      fill_buff(src_buf, num_bytes_per_iter, num_bytes_per_iter * i);
      ret = write(fd, src_buf, num_bytes_per_iter);
      assert(ret == num_bytes_per_iter);
    }
    fsync(fd);
    close(fd);
  }

  // reopen the file and check result
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  // check that the content before OFFSET are all zeros
  if (init_offset != 0) {
    char actual[init_offset];
    ret = read(fd, actual, init_offset);
    assert(ret == init_offset);

    char expected[init_offset];
    memset(expected, 0, init_offset);

    CHECK_RESULT(expected, actual, init_offset, fd);
  }

  // check that content after OFFSET are written
  {
    int length = num_bytes_per_iter * num_iter;

    char actual[length];
    ret = read(fd, actual, length);
    assert(ret == length);

    char expected[length];
    fill_buff(expected, length);

    CHECK_RESULT(expected, actual, length, fd);
  }

  fsync(fd);
  close(fd);
}

int main(int argc, char* argv[]) {
  // everything block-aligned
  test({.num_bytes_per_iter = BLOCK_SIZE});
  test({.num_bytes_per_iter = BLOCK_SIZE * 8});
  test({.num_bytes_per_iter = BLOCK_SIZE * 33});
  test({.num_bytes_per_iter = BLOCK_SIZE * 63});
  test({.num_bytes_per_iter = BLOCK_SIZE, .num_iter = 2});
  test({.num_bytes_per_iter = BLOCK_SIZE * 8, .num_iter = 2});

  // single-block write w/ block-aligned starting offset
  test({.num_bytes_per_iter = 8});
  test({.num_bytes_per_iter = 1});
  test({.num_bytes_per_iter = 17});
  test({.num_bytes_per_iter = 64});
  test({.num_bytes_per_iter = BLOCK_SIZE / 2});
  test({.num_bytes_per_iter = BLOCK_SIZE - 1});

  // single-block write w/o alignment
  test({.num_bytes_per_iter = 8, .init_offset = 8});
  test({.num_bytes_per_iter = 8, .init_offset = BLOCK_SIZE - 8});
  test({.num_bytes_per_iter = BLOCK_SIZE - 8, .init_offset = 8});
  test({.num_bytes_per_iter = BLOCK_SIZE / 2, .init_offset = BLOCK_SIZE / 2});
  test({.num_bytes_per_iter = 8,
        .num_iter = NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY + 1});
  test({.num_bytes_per_iter = 8, .num_iter = BLOCK_SIZE / 8});
  test({.num_bytes_per_iter = 8, .num_iter = BLOCK_SIZE * 2 / 8});
  test({.num_bytes_per_iter = 9, .num_iter = BLOCK_SIZE / 9});
  test({.num_bytes_per_iter = 9, .num_iter = BLOCK_SIZE * 2 / 9});
  test({.num_bytes_per_iter = 42, .num_iter = 12, .init_offset = 123});

  // multi-block write w/ block-aligned starting offset
  test({.num_bytes_per_iter = BLOCK_SIZE + 1});
  test({.num_bytes_per_iter = BLOCK_SIZE * 2 - 1});
  test({.num_bytes_per_iter = BLOCK_SIZE * 16 + 1});
  test({.num_bytes_per_iter = BLOCK_SIZE + 1, .init_offset = BLOCK_SIZE * 2});
  test({.num_bytes_per_iter = 12345, .init_offset = BLOCK_SIZE * 7});

  // multi-block write w/o alignment
  test({.num_bytes_per_iter = BLOCK_SIZE, .init_offset = 8});
  test({.num_bytes_per_iter = BLOCK_SIZE * 7 + 1, .init_offset = 8});
  test({.num_bytes_per_iter = BLOCK_SIZE + 1, .init_offset = 1});
  test({.num_bytes_per_iter = BLOCK_SIZE + 1, .init_offset = BLOCK_SIZE - 1});
  test({.num_bytes_per_iter = BLOCK_SIZE * 16 - 13, .init_offset = 13});
  test({.num_bytes_per_iter = 123, .num_iter = 6, .init_offset = 7890});

  // multi-block huge write w/o alignment
  test({.num_bytes_per_iter = 356791, .num_iter = 2, .init_offset = 542});
  test({.num_bytes_per_iter = 1300000, .init_offset = 17});

  return 0;
}
