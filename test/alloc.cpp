#include <fcntl.h>

#include <iostream>

#include "../src/lib.h"

constexpr auto FILEPATH = "test.txt";

int main(int argc, char* argv[]) {
  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  std::cout << "fd: " << fd << "\n";

  for (auto const& [_, file] : ulayfs::files) {
    std::cout << *file << std::endl;
  };
}