#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include "common.h"

constexpr int STR_LEN = ulayfs::BLOCK_SIZE * 16 + 1;
const std::string test_str(STR_LEN, 'a');
char buff[STR_LEN + 1]{};

size_t sz = 0;
int rc = 0;

void test_write() {
  fprintf(stderr, "test_write\n");

  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd >= 0);

  sz = write(fd, test_str.data(), test_str.length());
  assert(sz == test_str.length());

  rc = fsync(fd);
  assert(rc == 0);

  rc = close(fd);
  assert(rc == 0);
}

void test_read() {
  fprintf(stderr, "test_read\n");

  int fd = open(FILEPATH, O_RDONLY);
  assert(fd >= 0);

  size_t first_half = test_str.length() / 2;
  size_t second_half = test_str.length() - first_half;

  sz = read(fd, buff, first_half);
  assert(sz == first_half);
  assert(test_str.compare(0, first_half, buff) == 0);

  sz = read(fd, buff, test_str.length());
  assert(sz == second_half);
  assert(test_str.compare(first_half, second_half, buff) == 0);

  sz = read(fd, buff, test_str.length());
  assert(sz == 0);

  rc = close(fd);
  assert(rc == 0);
}

void test_mmap() {
  fprintf(stderr, "test_mmap\n");

  int fd = open(FILEPATH, O_RDWR);
  assert(fd >= 0);

  void* ptr = mmap(nullptr, test_str.length(), PROT_READ, MAP_SHARED, fd, 0);
  assert(ptr != MAP_FAILED);

  std::string_view result(static_cast<char*>(ptr), test_str.length());

  assert(result.compare(test_str) == 0);

  rc = munmap(ptr, test_str.length());
  assert(rc == 0);

  rc = close(fd);
  assert(rc == 0);
}

void test_lseek() {
  fprintf(stderr, "test_lseek\n");

  int fd = open(FILEPATH, O_RDWR);
  assert(fd >= 0);

  sz = write(fd, test_str.data(), test_str.length());
  assert(sz == test_str.length());

  {
    sz = lseek(fd, 0, SEEK_SET);
    assert(sz == 0);

    sz = read(fd, buff, test_str.length());
    assert(sz == test_str.length());
    assert(test_str == buff);
  }

  {
    sz = lseek(fd, -1, SEEK_CUR);
    assert(sz == test_str.length() - 1);

    sz = read(fd, buff, test_str.length());
    assert(sz == 1);
    assert(test_str[test_str.length() - 1] == buff[0]);
  }

  {
    sz = lseek(fd, -static_cast<off_t>(test_str.length()), SEEK_END);
    assert(sz == 0);

    sz = read(fd, buff, test_str.length());
    assert(sz == test_str.length());
    assert(test_str == buff);
  }

  rc = fsync(fd);
  assert(rc == 0);

  rc = close(fd);
  assert(rc == 0);
}

void test_stream() {
  fprintf(stderr, "test_stream\n");

  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  FILE* stream = fdopen(fd, "w");
  fclose(stream);
}

int main() {
  remove(FILEPATH);
  test_write();
  test_read();
  test_lseek();
#if ULAYFS_USE_PMEMCHECK == 0
  test_mmap();
#endif
  test_stream();
  return 0;
}
