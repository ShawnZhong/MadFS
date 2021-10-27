#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FILEPATH "test.txt"
#define TEST_STR "test str\n"

void test_write() {
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  ssize_t sz = write(fd, TEST_STR, strlen(TEST_STR));
  assert(sz == strlen(TEST_STR));

  int rc = close(fd);
  assert(rc == 0);
}

void test_read() {
  int fd = open(FILEPATH, O_RDWR);
  assert(fd >= 0);

  char buff[sizeof(TEST_STR)];
  ssize_t sz = read(fd, buff, sizeof(TEST_STR));
  assert(sz == strlen(TEST_STR));
  assert(strcmp(buff, TEST_STR) == 0);

  int rc = close(fd);
  assert(rc == 0);
}

int main() {
  remove(FILEPATH);
  test_write();
  test_read();
  return 0;
}