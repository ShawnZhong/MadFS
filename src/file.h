#pragma once

#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <tbb/concurrent_unordered_map.h>

#include <cstdint>
#include <ctime>
#include <iostream>
#include <stdexcept>

#include "alloc.h"
#include "bitmap.h"
#include "blk_table.h"
#include "block/block.h"
#include "config.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "mem_table.h"
#include "posix.h"
#include "tx/mgr.h"
#include "tx/offset.h"
#include "utils.h"

namespace ulayfs::utility {
class Converter;
}

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  Bitmap* bitmap;
  MemTable mem_table;
  pmem::MetaBlock* meta;
  TxMgr tx_mgr;
  BlkTable blk_table;

  int shm_fd;
  const bool can_read;
  const bool can_write;
  char shm_path[SHM_PATH_LEN];

  // each thread tid has its local allocator
  // the allocator is a per-thread per-file data structure
  tbb::concurrent_unordered_map<pid_t, Allocator> allocators;

  // move spinlock into a separated cacheline
  union {
    pthread_spinlock_t spinlock;
    char cl[CACHELINE_SIZE];
  };

 public:
  // only set at debug mode
  const char* path;

  // transformer will have to do many dirty and inclusive operations
  friend utility::Converter;

 public:
  File(int fd, const struct stat& stat, int flags, const char* pathname,
       bool guard = true);
  ~File();

  /*
   * POSIX I/O operations
   */
  ssize_t pwrite(const void* buf, size_t count, size_t offset);
  ssize_t write(const void* buf, size_t count);
  ssize_t pread(void* buf, size_t count, off_t offset);
  ssize_t read(void* buf, size_t count);
  off_t lseek(off_t offset, int whence);
  void* mmap(void* addr, size_t length, int prot, int flags, size_t offset);
  int fsync();
  void stat(struct stat* buf);

  /*
   * Getters
   */
  [[nodiscard]] Allocator* get_local_allocator();

  /*
   * exported interface for update; init_bitmap is always false
   */
  void update(FileState* state, bool do_alloc) {
    if (!blk_table.need_update(state, do_alloc)) return;
    pthread_spin_lock(&spinlock);
    blk_table.update(do_alloc);
    *state = blk_table.get_file_state();
    pthread_spin_unlock(&spinlock);
  }

  void update_with_offset(FileState* state, uint64_t& count,
                          bool stop_at_boundary, uint64_t& ticket,
                          uint64_t& old_offset, bool do_alloc) {
    pthread_spin_lock(&spinlock);
    blk_table.update(do_alloc);
    *state = blk_table.get_file_state();
    old_offset = tx_mgr.offset_mgr.acquire_offset(count, state->file_size,
                                                  stop_at_boundary, ticket);
    pthread_spin_unlock(&spinlock);
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
    pmem::Block* block = mem_table.get(lidx);
    assert(IS_ALIGNED(reinterpret_cast<uintptr_t>(block), BLOCK_SIZE));
    return block;
  }

  /**
   * @return a read-only pointer to the block given a logical block index
   * An empty block is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] const pmem::Block* lidx_to_addr_ro(LogicalBlockIdx lidx) {
    constexpr static const char __attribute__((aligned(BLOCK_SIZE)))
    empty_block[BLOCK_SIZE]{};
    if (lidx == 0) return reinterpret_cast<const pmem::Block*>(&empty_block);
    return lidx_to_addr_rw(lidx);
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
    bitmap[block_idx >> BITMAP_BLOCK_CAPACITY_SHIFT].set_allocated(
        block_idx & (BITMAP_BLOCK_CAPACITY - 1));
  }

  /**
   * Open the shared memory object corresponding to this file and save the
   * mmapped address to bitmap. The leading bit of the bitmap (corresponding to
   * metablock) indicates if the bitmap needs to be initialized.
   *
   * @param[in] stat stat of the original file
   * @return the file descriptor for the shared memory object on success
   */
  void open_shm(const struct stat& stat);

  /**
   * Remove the shared memory object associated with the current file.
   * Try best effort and report no error if unlink fails, since the shared
   * memory object might be removed by kernel.
   */
  void unlink_shm();

  /**
   * Remove the shared memory object associated for a given file
   * @param filepath the path of the file
   */
  static void unlink_shm(const char* filepath);

  void tx_gc();

  // try to open a file with checking whether the given file is in uLayFS format
  static bool try_open(int& fd, struct stat& stat_buf, const char* pathname,
                       int flags, mode_t mode) {
    if ((flags & O_ACCMODE) == O_WRONLY) {
      LOG_INFO("File \"%s\" opened with O_WRONLY. Changed to O_RDWR.",
               pathname);
      flags &= ~O_WRONLY;
      flags |= O_RDWR;
    }

    fd = posix::open(pathname, flags, mode);
    if (unlikely(fd < 0)) {
      LOG_WARN("File \"%s\" open failed: %m", pathname);
      return false;
    }

    if (!(flags & O_CREAT)) {
      // a non-empty file w/o shm_path cannot be a uLayFS file
      ssize_t rc = fgetxattr(fd, SHM_XATTR_NAME, nullptr, 0);
      if (rc == -1) return false;
    }

    int rc = posix::fstat(fd, &stat_buf);
    if (unlikely(rc < 0)) {
      LOG_WARN("File \"%s\" fstat failed: %m. Fallback to syscall.", pathname);
      return false;
    }

    // we don't handle non-normal file (e.g., socket, directory, block dev)
    if (unlikely(!S_ISREG(stat_buf.st_mode) && !S_ISLNK(stat_buf.st_mode))) {
      LOG_WARN("Non-normal file \"%s\". Fallback to syscall.", pathname);
      return false;
    }

    if (!IS_ALIGNED(stat_buf.st_size, BLOCK_SIZE)) {
      LOG_WARN("File size not aligned for \"%s\". Fallback to syscall",
               pathname);
      return false;
    }

    return true;
  }

  friend std::ostream& operator<<(std::ostream& out, const File& f);
};

}  // namespace ulayfs::dram
