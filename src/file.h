#pragma once

#include <stdexcept>

#include "config.h"
#include "layout.h"
#include "posix.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  int open_flags;
  pmem::MetaBlock* meta_block;

  // test if File is in a valid state
  explicit operator bool() const { return fd >= 0; }
  bool operator!() const { return fd < 0; }

 public:
  File() : fd(-1), meta_block(nullptr) {}
  int open(const char* pathname, int flags, mode_t mode);
};

}  // namespace ulayfs::dram
