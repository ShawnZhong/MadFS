#include "../src/util.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <iostream>

using namespace ulayfs::pmem;

constexpr auto FILEPATH = "test_util.txt";
char buffer[64];
int support_clwb = 0;

int main() {
  check_arch_support();

  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd > 0);

  int ret = posix_fallocate(fd, 0, 64);

  mmap(buffer, 64, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  for (int i = 0; i < 64; i++) {
    buffer[i] = 'B';
  }

  std::cout << "buffer: " << buffer << std::endl;

  persist(buffer, 64, true);
  return 0;
}