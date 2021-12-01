#pragma once

#include <sys/stat.h>

#include "bitmap.h"
#include "params.h"

namespace ulayfs {
extern void print_file(int fd) __attribute__((weak));
}  // namespace ulayfs
