#pragma once

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
  ALIGNED_TX_START,
  ALIGNED_TX_COMMIT,
  SINGLE_BLOCK_TX_START,
  SINGLE_BLOCK_TX_COPY,
  SINGLE_BLOCK_TX_COMMIT,
  MULTI_BLOCK_TX_START,
  MULTI_BLOCK_TX_COPY,
  MULTI_BLOCK_TX_COMMIT,
};

void count(Event event, size_t size = 0);
size_t get_count(Event event) __attribute__((weak));
void clear_count() __attribute__((weak));
void print_file(int fd) __attribute__((weak));
}  // namespace ulayfs::debug
