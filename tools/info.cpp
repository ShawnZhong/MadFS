/**
 * Show the metadata of a file.
 */
#include <iostream>

#include "lib/lib.h"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
    return 1;
  }

  const char *filename = argv[1];

  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    std::cerr << "Failed to open " << filename << ": " << strerror(errno)
              << std::endl;
    return 1;
  }

  if (auto file = ulayfs::get_file(fd)) {
    std::cerr << *file << "\n";
  } else {
    std::cerr << filename << " is not a uLayFS file. \n";
  }
}
