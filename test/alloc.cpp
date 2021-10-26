#include <fcntl.h>

#include <iostream>

#include "../src/lib.h"

constexpr auto FILEPATH = "test.txt";

int main(int argc, char* argv[]) {
  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char buf[4096];
  std::memset(buf, 'A', sizeof(buf));

  pwrite(fd, buf, sizeof(buf), 0);

  auto file = ulayfs::files[fd];
  std::cout << *file << std::endl;
}
