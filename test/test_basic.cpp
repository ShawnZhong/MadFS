#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include "common.h"

void test_write() {
  [[maybe_unused]] off_t res;

  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  ssize_t sz = write(fd, TEST_STR, TEST_STR_LEN);
  assert(sz == TEST_STR_LEN);

  res = fsync(fd);
  assert(res == 0);

  res = close(fd);
  assert(res == 0);
}

void test_read() {
  [[maybe_unused]] off_t res;

  assert(TEST_STR_LEN > 3);
  std::string test_str = TEST_STR;

  int fd = open(FILEPATH, O_RDWR);
  assert(fd >= 0);

  char buff[TEST_STR_LEN + 1]{};
  ssize_t sz = read(fd, buff, TEST_STR_LEN - 3);
  assert(sz == TEST_STR_LEN - 3);
  buff[sz] = '\0';
  assert(test_str.compare(0, TEST_STR_LEN - 3, buff) == 0);

  sz = read(fd, buff, 5);
  assert(sz == 3);
  buff[sz] = '\0';
  assert(test_str.compare(TEST_STR_LEN - 3, 3, buff) == 0);

  sz = read(fd, buff, 7);
  assert(sz == 0);

  res = fsync(fd);
  assert(res == 0);

  res = close(fd);
  assert(res == 0);
}

void test_lseek() {
  int fd = open(FILEPATH, O_RDWR);
  assert(fd >= 0);

  [[maybe_unused]] off_t res;
  char buff[sizeof(TEST_STR)]{};

  res = write(fd, TEST_STR, TEST_STR_LEN);
  assert(res == TEST_STR_LEN);

  res = lseek(fd, 0, SEEK_SET);
  assert(res == 0);

  res = read(fd, buff, TEST_STR_LEN);
  assert(res == TEST_STR_LEN);
  assert(strcmp(buff, TEST_STR) == 0);

  res = lseek(fd, -TEST_STR_LEN, SEEK_CUR);
  assert(res == 0);

  res = read(fd, buff, TEST_STR_LEN);
  assert(res == TEST_STR_LEN);
  assert(strcmp(buff, TEST_STR) == 0);

  res = lseek(fd, -TEST_STR_LEN, SEEK_END);
  assert(res == 0);

  res = read(fd, buff, TEST_STR_LEN);
  assert(res == TEST_STR_LEN);
  assert(strcmp(buff, TEST_STR) == 0);

  res = fsync(fd);
  assert(res == 0);

  print_file(fd);

  res = close(fd);
  assert(res == 0);
}

int main() {
  remove(FILEPATH);
  test_write();
  test_read();
  test_lseek();
  return 0;
}
