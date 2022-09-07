#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "common.h"

using ulayfs::debug::print_file;

constexpr int STR_LEN = ulayfs::BLOCK_SIZE * 16 + 123;
std::string test_str;
char buff[STR_LEN + 1]{};

size_t sz = 0;
int rc = 0;

const char* filepath = get_filepath();

void test_write() {
  fprintf(stderr, "test_write\n");

  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  ASSERT(fd >= 0);

  size_t len = test_str.length();
  size_t len_third = len / 3;

  sz = write(fd, test_str.data(), len_third);
  ASSERT(sz == len_third);

  sz = write(fd, test_str.data() + len_third, len_third);
  ASSERT(sz == len_third);

  sz = write(fd, test_str.data() + len_third * 2, len - len_third * 2);
  ASSERT(sz == len - len_third * 2);

  rc = fsync(fd);
  ASSERT(rc == 0);

  rc = close(fd);
  ASSERT(rc == 0);
}

void test_read() {
  fprintf(stderr, "test_read\n");

  int fd = open(filepath, O_RDONLY);
  ASSERT(fd >= 0);

  size_t first_half = test_str.length() / 2;
  size_t second_half = test_str.length() - first_half;

  sz = read(fd, buff, first_half);
  ASSERT(sz == first_half);
  ASSERT(test_str.compare(0, first_half, buff) == 0);

  sz = read(fd, buff, test_str.length());
  ASSERT(sz == second_half);
  ASSERT(test_str.compare(first_half, second_half, buff) == 0);

  sz = read(fd, buff, test_str.length());
  ASSERT(sz == 0);

  rc = close(fd);
  ASSERT(rc == 0);
}

void test_mmap() {
  fprintf(stderr, "test_mmap\n");

  int fd = open(filepath, O_RDONLY);
  ASSERT(fd >= 0);

  void* ptr = mmap(nullptr, test_str.length(), PROT_READ, MAP_SHARED, fd, 0);
  ASSERT(ptr != MAP_FAILED);

  std::string_view result(static_cast<char*>(ptr), test_str.length());

  ASSERT(result.compare(test_str) == 0);

  rc = munmap(ptr, test_str.length());
  ASSERT(rc == 0);

  rc = close(fd);
  ASSERT(rc == 0);
}

void test_lseek() {
  fprintf(stderr, "test_lseek\n");

  int fd = open(filepath, O_RDWR);
  ASSERT(fd >= 0);

  sz = write(fd, test_str.data(), test_str.length());
  ASSERT(sz == test_str.length());

  {
    sz = lseek(fd, 0, SEEK_SET);
    ASSERT(sz == 0);

    sz = read(fd, buff, test_str.length());
    ASSERT(sz == test_str.length());
    ASSERT(test_str == buff);
  }

  {
    sz = lseek(fd, -1, SEEK_CUR);
    ASSERT(sz == test_str.length() - 1);

    sz = read(fd, buff, test_str.length());
    ASSERT(sz == 1);
    ASSERT(test_str[test_str.length() - 1] == buff[0]);
  }

  {
    sz = lseek(fd, -static_cast<off_t>(test_str.length()), SEEK_END);
    ASSERT(sz == 0);

    sz = read(fd, buff, test_str.length());
    ASSERT(sz == test_str.length());
    ASSERT(test_str == buff);
  }

  rc = fsync(fd);
  ASSERT(rc == 0);

  rc = close(fd);
  ASSERT(rc == 0);
}

void test_stat() {
  fprintf(stderr, "test_stat\n");
  unlink(filepath);

  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  ASSERT(fd >= 0);

  struct stat stat_buf {};
  rc = fstat(fd, &stat_buf);
  ASSERT(rc == 0);
  ASSERT(stat_buf.st_size == 0);

  sz = write(fd, test_str.data(), test_str.length());
  ASSERT(sz == test_str.length());
  rc = fstat(fd, &stat_buf);
  ASSERT(rc == 0);
  ASSERT(stat_buf.st_size == static_cast<off_t>(test_str.length()));

  rc = close(fd);
  ASSERT(rc == 0);

  rc = stat(filepath, &stat_buf);
  ASSERT(rc == 0);
  ASSERT(stat_buf.st_size == static_cast<off_t>(test_str.length()));
}

void test_stream() {
  fprintf(stderr, "test_stream\n");

  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  ASSERT(fd >= 0);

  FILE* stream = fdopen(fd, "w");
  ASSERT(stream != nullptr);

  rc = fclose(stream);
  ASSERT(rc == 0);
}

void test_unlink() {
  fprintf(stderr, "test_unlink\n");

  rc = system("rm -rf /dev/shm/ulayfs_*");

  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  ASSERT(fd >= 0);

  rc = close(fd);
  ASSERT(rc == 0);

  rc = unlink(filepath);
  ASSERT(rc == 0);

  rc = system("find /dev/shm -maxdepth 1 -name ulayfs_* | grep .");
  ASSERT(rc != 0);  // make sure that there is no bitmap left
}

void test_print() {
  fprintf(stderr, "test_print\n");

  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  ASSERT(fd >= 0);

  sz = write(fd, test_str.data(), ulayfs::BLOCK_SIZE);
  ASSERT(sz == ulayfs::BLOCK_SIZE);

  sz = write(fd, test_str.data(), 8);
  ASSERT(sz == 8);

  rc = fsync(fd);
  ASSERT(rc == 0);

  print_file(fd);
  rc = close(fd);
  ASSERT(rc == 0);

  fd = open(filepath, O_RDONLY);
  ASSERT(fd >= 0);

  print_file(fd);
  rc = close(fd);
  ASSERT(rc == 0);
}

int main() {
  unsetenv("LD_PRELOAD");
  test_str = random_string(STR_LEN);
  unlink(filepath);

  test_write();
  test_read();
  test_lseek();
  test_mmap();
  test_stat();
  test_stream();
  test_unlink();
  test_print();
  return 0;
}
