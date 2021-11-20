#pragma once

#include <iostream>
#include <stdexcept>

#include "alloc.h"
#include "block.h"
#include "btable.h"
#include "config.h"
#include "entry.h"
#include "layout.h"
#include "log.h"
#include "mtable.h"
#include "posix.h"
#include "tx.h"
#include "utils.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  int open_flags;
  bool valid;
  off_t file_offset;

  pmem::MetaBlock* meta;
  MemTable* mem_table;
  BlkTable* blk_table;
  TxMgr tx_mgr;

  // each thread maintain a mapping from fd to allocator
  // the allocator is a per-thread per-file data structure
  thread_local static std::unordered_map<int, Allocator> allocators;

  // each thread maintain a mapping from fd to log managers
  thread_local static std::unordered_map<int, LogMgr> log_mgrs;

  friend class TxMgr;
  friend class LogMgr;
  friend class BlkTable;

 public:
  File(const char* pathname, int flags, mode_t mode);
  ~File();

  [[nodiscard]] bool is_valid() const { return valid; }
  [[nodiscard]] int get_fd() const { return fd; }

  /*
   * POSIX I/O operations
   */
  ssize_t pwrite(const void* buf, size_t count, size_t offset);
  ssize_t pread(void* buf, size_t count, off_t offset);
  off_t lseek(off_t offset, int whence);
  ssize_t write(const void* buf, size_t count);
  ssize_t read(void* buf, size_t count);

  /*
   * Getters for thread-local data structures
   */
  [[nodiscard]] Allocator* get_local_allocator();
  [[nodiscard]] LogMgr* get_local_log_mgr();

 private:
  /**
   * Return a write-only pointer to the block given a virtual block index
   * A nullptr is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] pmem::Block* vidx_to_addr_rw(VirtualBlockIdx vidx) const;

  /**
   * Return a read-only pointer to the block given a virtual block index
   * An empty block is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] const pmem::Block* vidx_to_addr_ro(VirtualBlockIdx vidx) const;

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
