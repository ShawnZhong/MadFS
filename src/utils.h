#pragma once

#include <bit>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <exception>

#include "config.h"

#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
// ref:
// https://github.com/llvm/llvm-project/blob/main/compiler-rt/include/sanitizer/msan_interface.h
#else
#define __msan_unpoison(...) ({})
#define __msan_scoped_disable_interceptor_checks(...) ({})
#define __msan_scoped_enable_interceptor_checks(...) ({})
#endif

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

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
    FPRINTF(log_file, "%s " msg, level_str, ##__VA_ARGS__); \
  } while (0)

#define TRACE(msg, ...) LOG(0, msg, ##__VA_ARGS__)
#define DEBUG(msg, ...) LOG(1, msg, ##__VA_ARGS__)
#define INFO(msg, ...) LOG(2, msg, ##__VA_ARGS__)
#define WARN(msg, ...) LOG(3, msg, ##__VA_ARGS__)

// adopted from `include/linux/align.h`
#define ALIGN_MASK(x, mask) (((x) + (mask)) & ~(mask))
#define ALIGN_UP(x, a) ALIGN_MASK((x), ((typeof(x))(a)-1))
#define ALIGN_DOWN(x, a) ((x) & ~((typeof(x))(a)-1))
#define IS_ALIGNED(x, a) (((x) & ((typeof(x))(a)-1)) == 0)

namespace ulayfs {

struct FileInitException : public std::exception {
  explicit FileInitException(const char *msg) : msg(msg) {}
  [[nodiscard]] const char *what() const noexcept override { return msg; }
  const char *msg;
};

struct FatalException : public std::exception {};

/**
 * @return the next power of 2 greater than x. If x is already a power of 2,
 * the next power of 2 is returned.
 */
template <typename T>
T next_pow2(T x) {
  // countl_zero counts the number of leading 0-bits in x
  return T(1) << (sizeof(T) * 8 - std::countl_zero(x));
}
}  // namespace ulayfs
