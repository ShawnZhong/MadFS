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
#include "utils.h"

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

 private:
  /**
   * Write data to the shadow page starting from start_logical_idx
   *
   * @param buf the buffer given by the user
   * @param count number of bytes in the buffer
   * @param start_offset the start offset within the first block
   */
  void write_data(const void* buf, size_t count, uint64_t start_offset,
                  pmem::VirtualBlockIdx& start_virtual_idx,
                  pmem::LogicalBlockIdx& start_logical_idx) {
    // the address of the start of the new blocks
    auto dst = mtable.get_addr(start_logical_idx)->data_block.data;

    // if the offset is not block-aligned, copy the remaining bytes at the
    // beginning to the shadow page
    if (start_offset) {
      auto src_idx = btable.get(start_virtual_idx);
      auto src = mtable.get_addr(src_idx)->data_block.data;
      memcpy(dst, src, start_offset);
    }

    // write the actual buffer
    memcpy(dst + start_offset, buf, count);

    // persist the changes
    pmem::persist_fenced(&dst, count + start_offset);
  }

 public:
  File() : fd(-1), meta(nullptr) {}

  // test if File is in a valid state
  explicit operator bool() const { return fd >= 0; }
  bool operator!() const { return fd < 0; }

  pmem::MetaBlock* get_meta() { return meta; }

  int get_fd() const { return fd; }

  // we use File::open to construct a File object instead of the standard
  // constructor since open may fail, and we want to report the return value
  // back to the caller
  int open(const char* pathname, int flags, mode_t mode);

  void overwrite(const void* buf, size_t count, size_t offset) {
    uint32_t num_blocks = ALIGN_UP(count, BLOCK_SIZE) >> BLOCK_SHIFT;
    pmem::VirtualBlockIdx start_virtual_idx = ALIGN_DOWN(offset, BLOCK_SIZE);
    pmem::LogicalBlockIdx start_logical_idx = allocator.alloc(num_blocks);

    auto tx_begin_idx = tx_mgr.begin_tx(start_virtual_idx, num_blocks);

    // TODO: handle the case where num_blocks > 64

    uint64_t start_offset = offset - start_virtual_idx * BLOCK_SIZE;
    write_data(buf, count, start_offset, start_virtual_idx, start_logical_idx);

    uint16_t last_remaining = num_blocks * BLOCK_SIZE - count - start_offset;
    auto log_entry_idx = tx_mgr.write_log_entry(
        start_virtual_idx, start_logical_idx, num_blocks, last_remaining);

    tx_mgr.commit_tx(tx_begin_idx, log_entry_idx);

    btable.update();
  }

  friend std::ostream& operator<<(std::ostream& out, const File& f) {
    out << "fd: " << f.fd << "\n";
    out << *f.meta;
    out << f.mtable;
    out << "\n";
    return out;
  }
};

}  // namespace ulayfs::dram
