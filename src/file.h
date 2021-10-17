#pragma once

#include <stdexcept>

#include "alloc.h"
#include "config.h"
#include "layout.h"
#include "map.h"
#include "posix.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  int open_flags;
  pmem::MetaBlock* meta;
  IdxMap idx_map;
  Allocator allocator;

 public:
  File() : fd(-1), meta(nullptr) {}

  // test if File is in a valid state
  explicit operator bool() const { return fd >= 0; }
  bool operator!() const { return fd < 0; }

  pmem::MetaBlock* get_meta() { return meta; }

  int get_fd() const { return fd; }

  int open(const char* pathname, int flags, mode_t mode);
};

}  // namespace ulayfs::dram
