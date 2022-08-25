#pragma once

#include <syscall.h>
#include <unistd.h>

#include "config.h"

/*
 * The following macros used for assertion and logging
 * Defined as macros since we want to have access to __FILE__ and __LINE__
 */
#define ULAYFS_FPRINTF(file, fmt, ...)                                      \
  do {                                                                      \
    auto now = std::chrono::high_resolution_clock::now();                   \
    std::chrono::duration<double> sec = now.time_since_epoch();             \
    const char *s = strrchr(__FILE__, '/');                                 \
    const char *caller_filename = s ? s + 1 : __FILE__;                     \
    fprintf(file, "[Thread %d] %f [%14s:%-3d] " fmt "\n", tid, sec.count(), \
            caller_filename, __LINE__, ##__VA_ARGS__);                      \
  } while (0)

// PANIC_IF is active for both debug and release modes
#define PANIC_IF(expr, msg, ...)                                  \
  do {                                                            \
    if (likely(!(expr))) break;                                   \
    ULAYFS_FPRINTF(stderr, "[PANIC] " msg ": %m", ##__VA_ARGS__); \
    assert(false);                                                \
    throw FatalException();                                       \
  } while (0)
#define PANIC(msg, ...) PANIC_IF(true, msg, ##__VA_ARGS__)

// LOG_* are not active in release mode
#define ULAYFS_LOG(level, msg, ...)                                \
  do {                                                             \
    if (level < runtime_options.log_level) break;                  \
    constexpr const char *level_str_arr[] = {                      \
        "[\u001b[37mTRACE\u001b[0m]",                              \
        "[\u001b[32mDEBUG\u001b[0m]",                              \
        "[\u001b[34mINFO\u001b[0m] ",                              \
        "[\u001b[31mWARN\u001b[0m] ",                              \
    };                                                             \
    constexpr const char *level_str = level_str_arr[level];        \
    if (log_file == nullptr) log_file = stderr;                    \
    ULAYFS_FPRINTF(log_file, "%s " msg, level_str, ##__VA_ARGS__); \
  } while (0)

#if ULAYFS_DEBUG
#define LOG_TRACE(msg, ...) ULAYFS_LOG(0, msg, ##__VA_ARGS__)
#define LOG_DEBUG(msg, ...) ULAYFS_LOG(1, msg, ##__VA_ARGS__)
#define LOG_INFO(msg, ...) ULAYFS_LOG(2, msg, ##__VA_ARGS__)
#define LOG_WARN(msg, ...) ULAYFS_LOG(3, msg, ##__VA_ARGS__)
#else
#define LOG_TRACE(msg, ...) ({})
#define LOG_DEBUG(msg, ...) ({})
#define LOG_INFO(msg, ...) ({})
#define LOG_WARN(msg, ...) ({})
#endif

namespace ulayfs {
inline __attribute__((tls_model("initial-exec"))) thread_local const pid_t tid =
    static_cast<pid_t>(syscall(SYS_gettid));
inline FILE *log_file = stderr;
}  // namespace ulayfs
