#include <fcntl.h>

#include <iostream>

#include "common.h"

using madfs::BLOCK_SIZE;
using madfs::NUM_INLINE_TX_ENTRY;
using madfs::NUM_TX_ENTRY_PER_BLOCK;

struct TestOpt {
  int num_bytes_per_iter = BLOCK_SIZE;
  int num_iter = 1;
  int init_offset = 0;
};

const char* filepath = get_filepath();

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
    char* src_buf = new char[num_bytes_per_iter];
    unlink(filepath);
    int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, init_offset, SEEK_SET);

    for (int i = 0; i < num_iter; ++i) {
      fill_buff(src_buf, num_bytes_per_iter, num_bytes_per_iter * i);
      ret = write(fd, src_buf, num_bytes_per_iter);
      ASSERT(ret == num_bytes_per_iter);
    }
    fsync(fd);
    close(fd);
    delete[] src_buf;
  }

  // reopen the file and check result
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  // check that the content before OFFSET are all zeros
  if (init_offset != 0) {
    char* actual = new char[init_offset];
    char* expected = new char[init_offset];

    ret = read(fd, actual, init_offset);
    ASSERT(ret == init_offset);
    memset(expected, 0, init_offset);

    CHECK_RESULT(expected, actual, init_offset, fd);
    delete[] actual;
    delete[] expected;
  }

  // check that content after OFFSET are written
  {
    int length = num_bytes_per_iter * num_iter;
    char* actual = new char[length];
    char* expected = new char[length];

    ret = read(fd, actual, length);
    ASSERT(ret == length);
    fill_buff(expected, length);

    CHECK_RESULT(expected, actual, length, fd);
    delete[] actual;
    delete[] expected;
  }

  fsync(fd);
  close(fd);
}

int main() {
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
        .num_iter = NUM_INLINE_TX_ENTRY + NUM_TX_ENTRY_PER_BLOCK + 1});
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

  // multi-block huge block-aligned write
  test({.num_bytes_per_iter = BLOCK_SIZE * 1024, .init_offset = 0});
  test({.num_bytes_per_iter = BLOCK_SIZE * 1024 * 4,
        .init_offset = BLOCK_SIZE * 3});

  return 0;
}
