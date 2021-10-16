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

 public:
  File() : fd(-1), meta_block(nullptr) {}

  // test if File is in a valid state
  explicit operator bool() const { return fd >= 0; }
  bool operator!() const { return fd < 0; }

  void lock() { meta_block->meta_lock.acquire(); }
  void unlock() { meta_block->meta_lock.release(); }

  uint32_t get_num_blocks() const { return meta_block->num_blocks; }
  void set_num_blocks(uint32_t num) { meta_block->num_blocks = num; }

  int get_fd() const { return fd; }

  int open(const char* pathname, int flags, mode_t mode);
};

}  // namespace ulayfs::dram
