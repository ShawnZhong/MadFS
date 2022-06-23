#pragma once

#include <magic_enum.hpp>
#include <map>
#include <mutex>
#include <unordered_map>

#include "config.h"

namespace ulayfs::debug {

/*
 * The following macros used for assertion and logging
 * Defined as macros since we want to have access to __FILE__ and __LINE__
 */

__attribute__((tls_model("initial-exec"))) extern thread_local const pid_t tid;
extern FILE *log_file;

#define FPRINTF(file, fmt, ...)                                       \
  do {                                                                \
    auto now = std::chrono::high_resolution_clock::now();             \
    std::chrono::duration<double> sec = now.time_since_epoch();       \
    const char *s = strrchr(__FILE__, '/');                           \
    const char *caller_filename = s ? s + 1 : __FILE__;               \
    fprintf(file, "[Thread %d] %f [%14s:%-3d] " fmt "\n", debug::tid, \
            sec.count(), caller_filename, __LINE__, ##__VA_ARGS__);   \
  } while (0)

// PANIC_IF is active for both debug and release modes
#define PANIC_IF(expr, msg, ...)                           \
  do {                                                     \
    if (likely(!(expr))) break;                            \
    FPRINTF(stderr, "[PANIC] " msg ": %m", ##__VA_ARGS__); \
    assert(false);                                         \
    throw FatalException();                                \
  } while (0)
#define PANIC(msg, ...) PANIC_IF(true, msg, ##__VA_ARGS__)

// TRACE, DEBUG, INFO, and WARN are not active in release mode
#define LOG(level, msg, ...)                                       \
  do {                                                             \
    if constexpr (!BuildOptions::debug) break;                     \
    if (level < runtime_options.log_level) break;                  \
    constexpr const char *level_str_arr[] = {                      \
        "[\u001b[37mTRACE\u001b[0m]",                              \
        "[\u001b[32mDEBUG\u001b[0m]",                              \
        "[\u001b[34mINFO\u001b[0m] ",                              \
        "[\u001b[31mWARN\u001b[0m] ",                              \
    };                                                             \
    constexpr const char *level_str = level_str_arr[level];        \
    if (debug::log_file == nullptr) debug::log_file = stderr;      \
    FPRINTF(debug::log_file, "%s " msg, level_str, ##__VA_ARGS__); \
  } while (0)

#define TRACE(msg, ...) LOG(0, msg, ##__VA_ARGS__)
#define DEBUG(msg, ...) LOG(1, msg, ##__VA_ARGS__)
#define INFO(msg, ...) LOG(2, msg, ##__VA_ARGS__)
#define WARN(msg, ...) LOG(3, msg, ##__VA_ARGS__)

enum Event {
  READ,
  WRITE,
  PREAD,
  PWRITE,
  OPEN,
  CLOSE,
  SINGLE_BLOCK_TX_START,
  SINGLE_BLOCK_TX_COPY,
  SINGLE_BLOCK_TX_COMMIT,
  MULTI_BLOCK_TX_START,
  MULTI_BLOCK_TX_COPY,
  MULTI_BLOCK_TX_COMMIT,
};

extern thread_local class Counter {
  static std::mutex print_mutex;
  std::array<size_t, magic_enum::enum_count<Event>()> counts;
  std::array<size_t, magic_enum::enum_count<Event>()> sizes;

 public:
  Counter() = default;
  ~Counter() {
    if constexpr (!BuildOptions::debug) return;
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

  void count(Event event, size_t size) {
    if constexpr (!BuildOptions::debug) return;
    counts[magic_enum::enum_integer(event)]++;
    sizes[magic_enum::enum_integer(event)] += size;
  }
} counter;

static inline void count(Event event, size_t size = 0) {
  counter.count(event, size);
}

void print_file(int fd) __attribute__((weak));
}  // namespace ulayfs::debug
