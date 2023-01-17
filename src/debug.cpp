#include "debug.h"

#include "lib/lib.h"
#include "utils/timer.h"

namespace madfs::debug {
void print_file(int fd) {
  __msan_scoped_disable_interceptor_checks();
  if (auto file = get_file(fd)) {
    std::cerr << *file << "\n";
  } else {
    std::cerr << "fd " << fd << " is not a MadFS file. \n";
  }

  __msan_scoped_enable_interceptor_checks();
}

size_t get_count(Event event) { return timer.get_count(event); }
size_t get_size(Event event) { return timer.get_size(event); }
std::chrono::nanoseconds get_duration(Event event) {
  return timer.get_duration(event);
}
void clear_timer() { timer.clear(); }
void print_timer() { timer.print(); }

}  // namespace madfs::debug
