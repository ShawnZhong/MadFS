#include <iostream>

#include "convert.h"
#include "file/file.h"
#include "lib/lib.h"
#include "posix.h"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
    return 1;
  }

  const char *filename = argv[1];

  int fd = madfs::posix::open(filename, O_RDWR);
  if (fd < 0) {
    std::cerr << "Failed to open " << filename << ": " << strerror(errno)
              << std::endl;
    return 1;
  }

  madfs::dram::File *file = madfs::utility::Converter::convert_to(fd, filename);
  delete file;

  return 0;
}
