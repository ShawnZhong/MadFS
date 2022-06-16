#include <syscall.h>
#include <unistd.h>

#include <iostream>
#include <unordered_map>

#include "file.h"
#include "lib.h"
#include "utils.h"

// The following variables are declared as `extern` in the header file, and
// defined here, so that we don't need to have a private (i.e., static) variable
// for every translation unit.
thread_local const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
FILE *log_file = stderr;

namespace ulayfs::debug {
thread_local class Counter counter;
std::mutex Counter::print_mutex;

void print_file(int fd) {
  __msan_scoped_disable_interceptor_checks();
  if (auto file = get_file(fd)) {
    std::cerr << *file << "\n";
  } else {
    std::cerr << "fd " << fd << " is not a uLayFS file. \n";
  }
  __msan_scoped_enable_interceptor_checks();
}
}  // namespace ulayfs::debug
