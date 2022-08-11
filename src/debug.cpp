#include "debug.h"

#include "lib.h"
#include "timer.h"

namespace ulayfs::debug {
void print_file(int fd) {
  __msan_scoped_disable_interceptor_checks();
  if (auto file = get_file(fd)) {
    std::cerr << *file << "\n";
  } else {
    std::cerr << "fd " << fd << " is not a uLayFS file. \n";
  }
  __msan_scoped_enable_interceptor_checks();
}

size_t get_occurrence(Event event) { return timer.get_occurrence(event); }
void clear_count() { timer.clear(); }
void print_counter() { timer.print(); }

}  // namespace ulayfs::debug
