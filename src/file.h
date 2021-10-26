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
    pmem::VirtualBlockIdx start_virtual_idx = ALIGN_DOWN(offset, BLOCK_SIZE);
    uint32_t num_blocks = ALIGN_UP(count, BLOCK_SIZE) >> BLOCK_SHIFT;

    auto tx_begin_idx = tx_mgr.begin_tx(start_virtual_idx, num_blocks);

    pmem::LogicalBlockIdx start_logical_idx = allocator.alloc(num_blocks);
    auto dst_data_block = mtable.get_addr(start_logical_idx)->data_block;

    // if the offset is not block-aligned, copy the remaining bytes at the
    // beginning to the shadow page
    uint64_t copy_offset = offset - start_virtual_idx * BLOCK_SIZE;
    if (copy_offset) {
      auto src_data_block_idx = btable.get(start_virtual_idx);
      auto src_data_block = mtable.get_addr(src_data_block_idx)->data_block;
      memcpy(dst_data_block.data, src_data_block.data, copy_offset);
    }
    memcpy(dst_data_block.data + copy_offset, buf, count);
    pmem::persist_fenced(&dst_data_block.data, count + copy_offset);

    uint16_t last_remaining = num_blocks * BLOCK_SIZE - count - copy_offset;
    auto log_entry_idx = tx_mgr.write_log_entry(
        start_virtual_idx, start_logical_idx, num_blocks, last_remaining);

    tx_mgr.commit_tx(tx_begin_idx, log_entry_idx);
  }

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
