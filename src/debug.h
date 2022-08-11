#pragma once

#include <cstddef>

namespace ulayfs {
enum class Event : std::size_t {
  READ,
  WRITE,
  PREAD,
  PWRITE,
  PWRITE_TX,
  OPEN,
  CLOSE,
  FSYNC,

  TX_ENTRY_INDIRECT,

  ALIGNED_TX,
  ALIGNED_TX_COPY,
  ALIGNED_TX_PREPARE,
  ALIGNED_TX_UPDATE,
  ALIGNED_TX_WAIT_OFFSET,
  ALIGNED_TX_COMMIT,

  SINGLE_BLOCK_TX_START,
  SINGLE_BLOCK_TX_COPY,
  SINGLE_BLOCK_TX_COMMIT,
  MULTI_BLOCK_TX_START,
  MULTI_BLOCK_TX_COPY,
  MULTI_BLOCK_TX_COMMIT,
};

// The following functions are intended for debug usage. They are declared as
// weak so other program can include this header without having to link with the
// library. One need to check `is_linked` before using them.
namespace debug {
void print_file(int fd) __attribute__((weak));
size_t get_occurrence(Event event) __attribute__((weak));
void clear_count() __attribute__((weak));
void print_counter() __attribute__((weak));
}  // namespace debug

static bool is_linked() { return debug::print_file != nullptr; }
}  // namespace ulayfs
