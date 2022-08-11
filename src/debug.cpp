#include "debug.h"

#include "counter.h"
#include "lib.h"

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

size_t get_occurrence(Event event) { return counter.get_occurrence(event); }
void clear_count() { counter.clear(); }
void print_counter() { counter.print(); }

}  // namespace ulayfs::debug
