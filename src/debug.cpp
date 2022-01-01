#include "lib.h"

namespace ulayfs {

void print_file(int fd) {
  __msan_scoped_disable_interceptor_checks();
  if (auto file = get_file(fd)) {
    std::cerr << *file << "\n";
  } else {
    std::cerr << "fd " << fd << " is not a uLayFS file. \n";
  }
  __msan_scoped_enable_interceptor_checks();
}
}  // namespace ulayfs
