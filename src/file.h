#pragma once

#include <tbb/concurrent_unordered_map.h>

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
#include "offset.h"
#include "posix.h"
#include "tx.h"
#include "utils.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  const int fd;
  Bitmap* bitmap;
  MemTable mem_table;
  pmem::MetaBlock* meta;
  TxMgr tx_mgr;
  BlkTable blk_table;
  OffsetMgr offset_mgr;

  int shm_fd;
  int flags;
  pthread_spinlock_t spinlock;

  // each thread tid has its local allocator
  // the allocator is a per-thread per-file data structure
  tbb::concurrent_unordered_map<pid_t, Allocator> allocators;

  // each thread tid has its local log_mgr
  // the log_mgr is a per-thread per-file data structure
  tbb::concurrent_unordered_map<pid_t, LogMgr> log_mgrs;

 public:
  File(int fd, const struct stat& stat, int flags);
  ~File();

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
   * Getters & removers for thread-local data structures
   */
  [[nodiscard]] Allocator* get_local_allocator();
  [[nodiscard]] LogMgr* get_local_log_mgr();

  /*
   * exported interface for update; init_bitmap is always false
   */
  void update(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
              uint64_t* new_file_size, bool do_alloc) {
    pthread_spin_lock(&spinlock);
    blk_table.update(tx_idx, tx_block, new_file_size, do_alloc);
    pthread_spin_unlock(&spinlock);
  }

  uint64_t update_with_offset(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                              int64_t offset_change, bool stop_at_boundary,
                              uint64_t& ticket, uint64_t* new_file_size,
                              bool do_alloc) {
    uint64_t new_file_size_local;
    pthread_spin_lock(&spinlock);
    blk_table.update(tx_idx, tx_block, &new_file_size_local, do_alloc);
    uint64_t ret = offset_mgr.acquire_offset(offset_change, new_file_size_local,
                                             stop_at_boundary, ticket);
    pthread_spin_unlock(&spinlock);
    if (new_file_size) *new_file_size = new_file_size_local;
    return ret;
  }

  bool wait_offset(uint64_t ticket, TxEntryIdx& prev_idx,
                   const pmem::TxBlock*& prev_block) {
    return offset_mgr.wait_offset(ticket, prev_idx, prev_block);
  }

  bool validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                       const pmem::TxBlock* curr_block, TxEntryIdx& prev_idx,
                       const pmem::TxBlock*& prev_block) {
    return offset_mgr.validate_offset(ticket, curr_idx, curr_block, prev_idx,
                                      prev_block);
  }

  void release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                      const pmem::TxBlock* curr_block) {
    return offset_mgr.release_offset(ticket, curr_idx, curr_block);
  }

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

  /**
   * Mark the logical block as allocated. This is not thread safe and should
   * only be used on startup if the bitmap is newly created.
   */
  void set_allocated(LogicalBlockIdx block_idx) {
    bitmap[block_idx >> BITMAP_CAPACITY_SHIFT].set_allocated(
        block_idx & (BITMAP_CAPACITY - 1));
  }

  /**
   * Open the shared memory object corresponding to this file and save the
   * mmapped address to bitmap. The leading bit of the bitmap (corresponding to
   * metablock) indicates if the bitmap needs to be initialized.
   *
   * @param[in] shm_path path to the shared memory object
   * @param[in] stat stat of the original file
   * @param[out] bitmap the bitmap opened or created in the shared memory
   * @return the file descriptor for the shared memory object on success,
   * -1 otherwise
   */
  int open_shm(const char* shm_path, const struct stat& stat, Bitmap*& bitmap);

  bool tx_idx_greater(const TxEntryIdx lhs_idx, const TxEntryIdx rhs_idx,
                      const pmem::TxBlock* lhs_block = nullptr,
                      const pmem::TxBlock* rhs_block = nullptr) {
    return tx_mgr.tx_idx_greater(lhs_idx, rhs_idx, lhs_block, rhs_block);
  }

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
