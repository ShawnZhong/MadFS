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

  friend class TxMgr;

  // each thread maintain a mapping from fd to allocator
  // the allocator is a per-thread per-file data structure
  thread_local static std::unordered_map<int, Allocator> allocators;

  // each thread maintain a mapping from fd to log managers
  thread_local static std::unordered_map<int, LogMgr> log_mgrs;

 public:
  File(const char* pathname, int flags, mode_t mode);
  ~File();

  [[nodiscard]] bool is_valid() const { return valid; }
  [[nodiscard]] int get_fd() const { return fd; }
  [[nodiscard]] Allocator* get_local_allocator();
  [[nodiscard]] LogMgr* get_local_log_mgr();

  /**
   * write the content in buf to the byte range [offset, offset + count)
   */
  ssize_t pwrite(const void* buf, size_t count, size_t offset);

  /**
   * read the byte range [offset, offset + count) to buf
   */
  ssize_t pread(void* buf, size_t count, off_t offset);

  /**
   * reposition read/write file offset
   */
  off_t lseek(off_t offset, int whence);

  /**
   * write the content in buf to the byte range [file_offset, file_offset +
   * count)
   */
  ssize_t write(const void* buf, size_t count);

  /**
   * read the byte range [file_offset, file_offset + count) to buf
   */
  ssize_t read(void* buf, size_t count);

 private:
  /**
   * Given a virtual block index, return a read-only data pointer
   *
   * @param virtual_block_idx the virtual block index for a data block
   * @return the const char pointer pointing to the memory location of the data
   * block. An empty block if the block is not allocated yet (e.g., a hole)
   */
  const char* get_ro_data_ptr(VirtualBlockIdx virtual_block_idx);

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
