#pragma once

#include <iostream>
#include <stdexcept>

#include "alloc.h"
#include "block.h"
#include "btable.h"
#include "config.h"
#include "entry.h"
#include "idx.h"
#include "log.h"
#include "mtable.h"
#include "posix.h"
#include "tx.h"
#include "utils.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  const int fd;
  pmem::Bitmap* bitmap;
  MemTable mem_table;
  pmem::MetaBlock* meta;
  TxMgr tx_mgr;
  BlkTable blk_table;

  int shm_fd;
  off_t file_offset;
  int flags;

  // each thread maintain a mapping from fd to allocator
  // the allocator is a per-thread per-file data structure
  thread_local static std::unordered_map<int, Allocator> allocators;

  // each thread maintain a mapping from fd to log managers
  thread_local static std::unordered_map<int, LogMgr> log_mgrs;

  friend class TxMgr;
  friend class LogMgr;
  friend class BlkTable;

 public:
  File(int fd, off_t init_file_size, pmem::Bitmap* bitmap, int shm_fd,
       int flags);
  ~File() {
    DEBUG("posix::close(%d)", fd);
    posix::close(fd);
    posix::close(shm_fd);
    allocators.erase(fd);
    log_mgrs.erase(fd);
  }

  /*
   * POSIX I/O operations
   */
  ssize_t pwrite(const void* buf, size_t count, size_t offset);
  ssize_t write(const void* buf, size_t count);
  ssize_t pread(void* buf, size_t count, off_t offset);
  ssize_t read(void* buf, size_t count);
  off_t lseek(off_t offset, int whence);
  int fsync();

  /*
   * Getters for thread-local data structures
   */
  [[nodiscard]] Allocator* get_local_allocator();
  [[nodiscard]] LogMgr* get_local_log_mgr();

 private:
  /**
   * @return the logical block index corresponding to the virtual index
   */
  [[nodiscard]] LogicalBlockIdx vidx_to_lidx(VirtualBlockIdx vidx) {
    return blk_table.get(vidx);
  }

  /**
   * @return a writable pointer to the block given a logical block index
   * A nullptr is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] pmem::Block* lidx_to_addr_rw(LogicalBlockIdx lidx) {
    return mem_table.get(lidx);
  }

  /**
   * @return a read-only pointer to the block given a logical block index
   * An empty block is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] const pmem::Block* lidx_to_addr_ro(LogicalBlockIdx lidx) {
    constexpr static const char empty_block[BLOCK_SIZE]{};
    if (lidx == 0) return reinterpret_cast<const pmem::Block*>(&empty_block);
    return mem_table.get(lidx);
  }

  /**
   * @return a writable pointer to the block given a virtual block index
   * A nullptr is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] pmem::Block* vidx_to_addr_rw(VirtualBlockIdx vidx) {
    return lidx_to_addr_rw(vidx_to_lidx(vidx));
  }

  /**
   * @return a read-only pointer to the block given a virtual block index
   * An empty block is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] const pmem::Block* vidx_to_addr_ro(VirtualBlockIdx vidx) {
    return lidx_to_addr_ro(vidx_to_lidx(vidx));
  }

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
