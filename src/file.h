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

#include "alloc/alloc.h"
#include "bitmap.h"
#include "blk_table.h"
#include "block/block.h"
#include "config.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "mem_table.h"
#include "offset.h"
#include "posix.h"
#include "shm.h"
#include "tx/mgr.h"
#include "utils/utils.h"

namespace ulayfs::utility {
class Converter;
class GarbageCollector;
}  // namespace ulayfs::utility

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
 public:
  BitmapMgr bitmap_mgr;
  MemTable mem_table;
  OffsetMgr offset_mgr;
  TxMgr tx_mgr;
  BlkTable blk_table;
  ShmMgr shm_mgr;
  pmem::MetaBlock* const meta;
  const char* path;  // only set at debug mode

 private:
  int fd;
  const bool can_read;
  const bool can_write;

  // each thread tid has its local allocator
  // the allocator is a per-thread per-file data structure
  tbb::concurrent_unordered_map<pid_t, Allocator> allocators;

  // move spinlock into a separated cacheline
  union {
    pthread_spinlock_t spinlock;
    char cl[CACHELINE_SIZE];
  };

  // transformer will have to do many dirty and inclusive operations
  friend utility::Converter;

 public:
  File(int fd, const struct stat& stat, int flags, const char* pathname);
  ~File();

  /*
   * POSIX I/O operations
   */
  ssize_t pwrite(const void* buf, size_t count, size_t offset);
  ssize_t write(const void* buf, size_t count);
  ssize_t pread(void* buf, size_t count, off_t offset);
  ssize_t read(void* buf, size_t count);
  off_t lseek(off_t offset, int whence);
  void* mmap(void* addr, size_t length, int prot, int flags,
             size_t offset) const;
  int fsync();
  void stat(struct stat* buf);

  /*
   * Getters
   */
  [[nodiscard]] Allocator* get_local_allocator();

  void update(FileState* state, Allocator* allocator = nullptr) {
    if (!blk_table.need_update(state, allocator)) return;
    pthread_spin_lock(&spinlock);
    blk_table.update(allocator);
    *state = blk_table.get_file_state();
    pthread_spin_unlock(&spinlock);
  }

  void update_with_offset(FileState* state, uint64_t& count,
                          bool stop_at_boundary, uint64_t& ticket,
                          uint64_t& old_offset,
                          Allocator* allocator = nullptr) {
    pthread_spin_lock(&spinlock);
    blk_table.update(allocator);
    *state = blk_table.get_file_state();
    old_offset =
        offset_mgr.acquire(count, state->file_size, stop_at_boundary, ticket);
    pthread_spin_unlock(&spinlock);
  }

  /**
   * @return the logical block index corresponding to the virtual index
   */
  [[nodiscard]] LogicalBlockIdx vidx_to_lidx(VirtualBlockIdx vidx) const {
    return blk_table.get(vidx);
  }

  /**
   * @return a read-only pointer to the block given a virtual block index
   * An empty block is returned if the block is not allocated yet (e.g., a hole)
   */
  [[nodiscard]] const pmem::Block* vidx_to_addr_ro(VirtualBlockIdx vidx) {
    return mem_table.lidx_to_addr_ro(vidx_to_lidx(vidx));
  }

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
