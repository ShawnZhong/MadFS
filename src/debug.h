#pragma once

#include <map>
#include <mutex>
#include <unordered_map>

#include "config.h"

/*
 * The following macros used for assertion and logging
 * Defined as macros since we want to have access to __FILE__ and __LINE__
 */

__attribute__((tls_model("initial-exec"))) extern thread_local const pid_t tid;
extern FILE *log_file;

#define FPRINTF(file, fmt, ...)                                             \
  do {                                                                      \
    auto now = std::chrono::high_resolution_clock::now();                   \
    std::chrono::duration<double> sec = now.time_since_epoch();             \
    const char *s = strrchr(__FILE__, '/');                                 \
    const char *caller_filename = s ? s + 1 : __FILE__;                     \
    fprintf(file, "[Thread %d] %f [%14s:%-3d] " fmt "\n", tid, sec.count(), \
            caller_filename, __LINE__, ##__VA_ARGS__);                      \
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
#ifdef NDEBUG
#define LOG(...) ({})
#else
#define LOG(level, msg, ...)                                \
  do {                                                      \
    if constexpr (!BuildOptions::debug) break;              \
    if (level < runtime_options.log_level) break;           \
    constexpr const char *level_str_arr[] = {               \
        "[\u001b[37mTRACE\u001b[0m]",                       \
        "[\u001b[32mDEBUG\u001b[0m]",                       \
        "[\u001b[34mINFO\u001b[0m] ",                       \
        "[\u001b[31mWARN\u001b[0m] ",                       \
    };                                                      \
    constexpr const char *level_str = level_str_arr[level]; \
    if (log_file == nullptr) log_file = stderr;             \
    FPRINTF(log_file, "%s " msg, level_str, ##__VA_ARGS__); \
  } while (0)
#endif

#define TRACE(msg, ...) LOG(0, msg, ##__VA_ARGS__)
#define DEBUG(msg, ...) LOG(1, msg, ##__VA_ARGS__)
#define INFO(msg, ...) LOG(2, msg, ##__VA_ARGS__)
#define WARN(msg, ...) LOG(3, msg, ##__VA_ARGS__)

namespace ulayfs::debug {

extern thread_local class Counter {
  static std::mutex print_mutex;
  std::unordered_map<std::string, size_t> counter;

 public:
  Counter() = default;
  ~Counter() {
    if constexpr (!BuildOptions::debug) return;
    std::lock_guard<std::mutex> guard(print_mutex);
    fprintf(log_file, "[%d] Counters:\n", tid);
    for (auto &[name, size] : std::map{counter.begin(), counter.end()}) {
      fprintf(log_file, "\t%-25s %12zu\n", name.c_str(), size);
    }
  }

  void count(const std::string &name) {
    if constexpr (!BuildOptions::debug) return;
    counter[name]++;
  }

  void count(const std::string &name, size_t size) {
    if constexpr (!BuildOptions::debug) return;
    counter[name + "::size"] += size;
    counter[name + "::count"]++;
  }
} counter;

void print_file(int fd) __attribute__((weak));
}  // namespace ulayfs::debug
