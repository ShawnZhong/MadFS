#include <iostream>

#include "file.h"
#include "lib.h"
#include "posix.h"
#include "convert.h"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
    return 1;
  }

  const char *filename = argv[1];

  int fd = ulayfs::posix::open(filename, O_RDWR);
  if (fd < 0) {
    std::cerr << "Failed to open " << filename << ": " << strerror(errno)
              << std::endl;
    return 1;
  }

  ulayfs::dram::File *file = ulayfs::utility::Converter::convert_to(fd);
  delete file;

  return 0;
}
