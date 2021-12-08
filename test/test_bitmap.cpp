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
  int num_tx = ulayfs::NUM_INLINE_TX_ENTRY + ulayfs::NUM_TX_ENTRY + 1;
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

  print_file(fd);

  res = close(fd);
  assert(res == 0);
}

void check_bitmap() {
  // reopen the file to build the dram bitmap
  int fd = open(FILEPATH, O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  // to ensure that dram and pmem bitmaps match
  print_file(fd);

  close(fd);
}

void cleanup() {
  int rc = unlink(FILEPATH);
  assert(rc == 0);

  rc = unlink(shm_path);
  assert(rc == 0);
}

int main() {
  unlink(FILEPATH);
  create_file();
  check_bitmap();

  // remove the shared memory object so that it will recreate a new bitmap on
  // next opening.
  int res = unlink(shm_path);
  assert(res == 0);
  check_bitmap();

  cleanup();
  return 0;
}
