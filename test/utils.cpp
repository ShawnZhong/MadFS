#include "../src/utils.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cassert>

using namespace ulayfs::pmem;

constexpr auto FILEPATH = "test_util.txt";

int main() {
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  assert(fd > 0);

  int rc = ftruncate(fd, 64);
  assert(rc == 0);

  void* addr = mmap(nullptr, 64, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  assert(addr != (void*)-1);

  auto buff = static_cast<char*>(addr);

  for (int i = 0; i < 64; i++) {
    buff[i] = 'B';
  }

  persist_fenced(buff + 1, 32);

  return 0;
}
