#include <syscall.h>
#include <unistd.h>

#include <iostream>
#include <magic_enum.hpp>
#include <mutex>

#include "file.h"
#include "lib.h"
#include "utils.h"

namespace ulayfs::debug {
// The following variables are declared as `extern` in the header file, and
// defined here, so that we don't need to have a private (i.e., static) variable
// for every translation unit.
thread_local const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
FILE *log_file = stderr;

struct Counter {
  static std::mutex print_mutex;
  std::array<size_t, magic_enum::enum_count<Event>()> counts;
  std::array<size_t, magic_enum::enum_count<Event>()> sizes;

  Counter() = default;
  ~Counter() { print(); }

  void count(Event event, size_t size) {
    counts[magic_enum::enum_integer(event)]++;
    sizes[magic_enum::enum_integer(event)] += size;
  }

  void clear() {
    counts.fill(0);
    sizes.fill(0);
  }

  bool is_empty() {
    for (auto count : counts)
      if (count != 0) return false;
    return true;
  }

  void print() {
    if constexpr (!BuildOptions::enable_counter) return;
    if (is_empty()) return;
    std::lock_guard<std::mutex> guard(print_mutex);
    fprintf(log_file, "    [Thread %d] Counters:\n", tid);
    magic_enum::enum_for_each<Event>([&](Event event) {
      auto val = magic_enum::enum_integer(event);
      size_t count = counts[val];
      if (count == 0) return;
      size_t size = sizes[val];
      fprintf(log_file, "        %-25s: %zu",
              magic_enum::enum_name(event).data(), count);
      if (size != 0) {
        double total_mb = (double)size / 1024.0 / 1024.0;
        double avg_kb = (double)size / 1024.0 / (double)count;
        fprintf(log_file, " (%.2f MB, avg = %.2f KB) ", total_mb, avg_kb);
      }
      fprintf(log_file, "\n");
    });
  }
};

thread_local Counter counter;

void count(Event event, size_t size) {
  if constexpr (BuildOptions::enable_counter) {
    counter.count(event, size);
  }
}

size_t get_count(Event event) {
  if constexpr (BuildOptions::enable_counter) {
    return counter.counts[event];
  } else {
    return 0;
  }
}

void clear_count() {
  if constexpr (BuildOptions::enable_counter) {
    counter.clear();
  }
}

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
