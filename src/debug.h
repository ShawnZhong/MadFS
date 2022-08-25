#pragma once

#include <cstddef>

#include "const.h"

namespace ulayfs {

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
