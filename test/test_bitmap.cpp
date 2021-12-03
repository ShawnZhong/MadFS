#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>

#include "common.h"

void create_file() {
  off_t res;
  ssize_t sz;

  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  sz = write(fd, TEST_STR, TEST_STR_LEN);
  assert(sz == TEST_STR_LEN);

  sz = write(fd, TEST_STR, TEST_STR_LEN);
  assert(sz == TEST_STR_LEN);

  sz = write(fd, TEST_STR, TEST_STR_LEN);
  assert(sz == TEST_STR_LEN);

  res = fsync(fd);
  assert(res == 0);

  res = close(fd);
  assert(res == 0);
}

int main() {
  create_file();

  return 0;
}
