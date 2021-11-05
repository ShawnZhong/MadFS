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
  Allocator allocator;
  MemTable mem_table;
  BlkTable blk_table;
  LogMgr log_mgr;
  TxMgr tx_mgr;

 public:
  File(const char* pathname, int flags, mode_t mode);
  ~File();

  [[nodiscard]] bool is_valid() const { return valid; }
  [[nodiscard]] pmem::MetaBlock* get_meta() { return meta; }
  [[nodiscard]] int get_fd() const { return fd; }

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
   * @param virtual_block_idx the virtual block index for a data block
   * @return the char pointer pointing to the memory location of the data block
   */
  const char* get_ro_data_ptr(VirtualBlockIdx virtual_block_idx);

 public:
  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
