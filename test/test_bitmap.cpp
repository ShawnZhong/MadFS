#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>

#include "common.h"
#include "const.h"
#include "posix.h"

char shm_path[4096];
int num_blocks;

void create_file() {
  off_t res;
  ssize_t sz;

  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  // create enough tx so that valid tx span beyond meta block
  //   int num_tx = ulayfs::NUM_INLINE_TX_ENTRY + 1;
  int num_tx = 1;
  for (int i = 0; i < num_tx; i++) {
    sz = write(fd, TEST_STR, TEST_STR_LEN);
    assert(sz == TEST_STR_LEN);
  }

  res = fsync(fd);
  assert(res == 0);

  struct stat stat;
  int rc = ulayfs::posix::fstat(fd, &stat);
  assert(rc == 0);
  sprintf(shm_path, "/dev/shm/ulayfs_%ld%ld%ld", stat.st_ino,
          stat.st_ctim.tv_sec, stat.st_ctim.tv_nsec);
  num_blocks = stat.st_blocks;

  res = close(fd);
  assert(res == 0);
}

void check_bitmap() {
  // remove the shared memory object so that it will recreate a new bitmap on
  // next opening.
  int res = unlink(shm_path);
  //  assert(res == 0);

  // reopen the file to build the dram bitmap
  int fd = open(FILEPATH, O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  print_file(fd);

  size_t sz;

  // this file is small so all bitmap should fit into inline bitmap
  char pmem_bitmap[ulayfs::BLOCK_SIZE];
  off_t inline_bitmap_begin = ulayfs::CACHELINE_SIZE * 2;
  size_t inline_bitmap_size = ulayfs::INLINE_BITMAP_CAPACITY / 8;
  sz = ulayfs::posix::pread(fd, pmem_bitmap, inline_bitmap_size,
                            inline_bitmap_begin);
  assert(sz == inline_bitmap_size);

  // read dram bitmap directly
  char dram_bitmap[ulayfs::BLOCK_SIZE];
  int shm_fd = ulayfs::posix::open(shm_path, O_RDWR, S_IRUSR | S_IWUSR);
  assert(shm_fd >= 0);
  sz = ulayfs::posix::pread(shm_fd, dram_bitmap, ulayfs::BLOCK_SIZE, 0);
  assert(sz == ulayfs::BLOCK_SIZE);

  // compare if they are equal for the first few bits corresponding to the valid
  // blocks
  for (int i = 0; i < num_blocks; i++) {
    int pmem_val = pmem_bitmap[i / 8] & (1 << (i % 8));
    int dram_val = dram_bitmap[i / 8] & (1 << (i % 8));
    if (pmem_val != dram_val || dram_val != 0)
      printf("i: %d - pmem_val: %d; dram_val: %d\n", i, pmem_val, dram_val);
  }

  close(fd);
  close(shm_fd);
}

void cleanup() {
  int rc = unlink(FILEPATH);
  assert(rc == 0);

  rc = unlink(shm_path);
  assert(rc == 0);
}

int main() {
  create_file();
  check_bitmap();
  cleanup();
  return 0;
}
