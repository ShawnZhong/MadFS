#pragma once

#include <iostream>
#include <stdexcept>

#include "alloc.h"
#include "btable.h"
#include "config.h"
#include "layout.h"
#include "mtable.h"
#include "posix.h"
#include "tx.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  int open_flags;
  pmem::MetaBlock* meta;
  MemTable mtable;
  BlkTable btable;
  Allocator allocator;
  TxMgr tx_mgr;

 public:
  File() : fd(-1), meta(nullptr) {}

  // test if File is in a valid state
  explicit operator bool() const { return fd >= 0; }
  bool operator!() const { return fd < 0; }

  pmem::MetaBlock* get_meta() { return meta; }

  int get_fd() const { return fd; }

  int open(const char* pathname, int flags, mode_t mode);

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
