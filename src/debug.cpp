#include "lib.h"

namespace ulayfs {

void print_file(int fd) {
  if (auto file = get_file(fd)) {
    std::cerr << *file << "\n";
  } else {
    std::cerr << "fd " << fd << " is not a uLayFS file. \n";
  }
}
}  // namespace ulayfs
