#include "file.h"

#include <sys/mman.h>
#include <sys/xattr.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>

#include "alloc/alloc.h"
#include "config.h"
#include "idx.h"
#include "shm.h"
#include "timer.h"
#include "utils.h"

namespace ulayfs::dram {

File::File(int fd, const struct stat& stat, int flags,
           const char* pathname [[maybe_unused]])
    : mem_table(fd, stat.st_size, (flags & O_ACCMODE) == O_RDONLY),
      tx_mgr(this, &mem_table),
      blk_table(&mem_table),
      shm_mgr(fd, stat, mem_table.get_meta()),
      meta(mem_table.get_meta()),
      fd(fd),
      can_read((flags & O_ACCMODE) == O_RDONLY ||
               (flags & O_ACCMODE) == O_RDWR),
      can_write((flags & O_ACCMODE) == O_WRONLY ||
                (flags & O_ACCMODE) == O_RDWR) {
  pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
  if (stat.st_size == 0) meta->init();

  uint64_t file_size;
  bool file_size_updated = false;

  // only open shared memory if we may write
  if (can_write) {
    bitmap_mgr.entries = static_cast<BitmapEntry*>(shm_mgr.get_bitmap_addr());

    // The first bit corresponds to the meta block which should always be set
    // to 1. If it is not, then bitmap needs to be initialized.
    // BitmapEntry::is_allocated is not thread safe but we don't yet have
    // concurrency
    if (!bitmap_mgr.entries[0].is_allocated(0)) {
      meta->lock();
      if (!bitmap_mgr.entries[0].is_allocated(0)) {
        file_size = blk_table.update(/*allocator=*/nullptr, &bitmap_mgr);
        file_size_updated = true;
        bitmap_mgr.entries[0].set_allocated(0);
      }
      meta->unlock();
    }
  }

  if (!file_size_updated) file_size = blk_table.update();

  if (flags & O_APPEND)
    tx_mgr.offset_mgr.seek_absolute(static_cast<off_t>(file_size));
  if constexpr (BuildOptions::debug) {
    path = strdup(pathname);
  }
}

File::~File() {
  pthread_spin_destroy(&spinlock);
  allocators.clear();
  if (fd >= 0) posix::close(fd);
  if constexpr (BuildOptions::debug) {
    free((void*)path);
  }
}

/*
 * POSIX I/O operations
 */

ssize_t File::pwrite(const void* buf, size_t count, size_t offset) {
  if (unlikely(!can_write)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  return tx_mgr.do_pwrite(static_cast<const char*>(buf), count, offset);
}

ssize_t File::write(const void* buf, size_t count) {
  if (unlikely(!can_write)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  return tx_mgr.do_write(static_cast<const char*>(buf), count);
}

ssize_t File::pread(void* buf, size_t count, off_t offset) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  return tx_mgr.do_pread(static_cast<char*>(buf), count,
                         static_cast<size_t>(offset));
}

ssize_t File::read(void* buf, size_t count) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  return tx_mgr.do_read(static_cast<char*>(buf), count);
}

off_t File::lseek(off_t offset, int whence) {
  int64_t ret;

  pthread_spin_lock(&spinlock);
  uint64_t file_size = blk_table.update();

  switch (whence) {
    case SEEK_SET:
      ret = tx_mgr.offset_mgr.seek_absolute(offset);
      break;
    case SEEK_CUR:
      ret = tx_mgr.offset_mgr.seek_relative(offset);
      if (ret == -1) errno = EINVAL;
      break;
    case SEEK_END:
      ret = tx_mgr.offset_mgr.seek_absolute(static_cast<off_t>(file_size) +
                                            offset);
      break;
    case SEEK_DATA:
    case SEEK_HOLE:
    default:
      ret = -1;
      errno = EINVAL;
  }

  pthread_spin_unlock(&spinlock);
  return ret;
}

void* File::mmap(void* addr_hint, size_t length, int prot, int mmap_flags,
                 size_t offset) const {
  if (offset % BLOCK_SIZE != 0) {
    errno = EINVAL;
    return MAP_FAILED;
  }

  // reserve address space by memory-mapping /dev/zero
  static int zero_fd = posix::open("/dev/zero", O_RDONLY);
  if (zero_fd == -1) {
    LOG_WARN("open(/dev/zero) failed");
    return MAP_FAILED;
  }
  void* res = posix::mmap(addr_hint, length, prot, mmap_flags, zero_fd, 0);
  if (res == MAP_FAILED) {
    LOG_WARN("mmap failed: %m");
    return MAP_FAILED;
  }
  char* new_addr = reinterpret_cast<char*>(res);
  char* old_addr = reinterpret_cast<char*>(meta);
  // TODO: there is a kernel bug that when the old_addr is unmapped, accessing
  //  new_addr results in kernel panic

  auto remap = [&old_addr, &new_addr](LogicalBlockIdx lidx,
                                      VirtualBlockIdx vidx,
                                      uint32_t num_blocks) {
    char* old_block_addr = old_addr + BLOCK_IDX_TO_SIZE(lidx);
    char* new_block_addr = new_addr + BLOCK_IDX_TO_SIZE(vidx);
    size_t len = BLOCK_NUM_TO_SIZE(num_blocks);
    int flag = MREMAP_MAYMOVE | MREMAP_FIXED;

    void* ret = posix::mremap(old_block_addr, len, len, flag, new_block_addr);
    return ret == new_block_addr;
  };

  // remap the blocks in the file
  VirtualBlockIdx vidx_end =
      BLOCK_SIZE_TO_IDX(ALIGN_UP(offset + length, BLOCK_SIZE));
  VirtualBlockIdx vidx_group_begin = BLOCK_SIZE_TO_IDX(offset);
  LogicalBlockIdx lidx_group_begin = blk_table.get(vidx_group_begin);
  uint32_t num_blocks = 0;
  for (VirtualBlockIdx vidx = vidx_group_begin; vidx < vidx_end; ++vidx) {
    LogicalBlockIdx lidx = blk_table.get(vidx);
    if (lidx == 0) PANIC("hole vidx=%d in mmap", vidx.get());

    if (lidx == lidx_group_begin + num_blocks) {
      num_blocks++;
      continue;
    }

    if (!remap(lidx_group_begin, vidx_group_begin, num_blocks)) goto error;

    lidx_group_begin = lidx;
    vidx_group_begin = vidx;
    num_blocks = 1;
  }

  if (!remap(lidx_group_begin, vidx_group_begin, num_blocks)) goto error;

  return new_addr;

error:
  LOG_WARN("remap failed: %m");
  posix::munmap(new_addr, length);
  return MAP_FAILED;
}

int File::fsync() {
  FileState state;
  this->update(&state);
  TxCursor::flush_up_to(&mem_table, state.cursor);
  // we keep an invariant that tx_tail must be a valid (non-overflow) idx
  // an overflow index implies that the `next` pointer of the block is not set
  // (and thus not flushed) yet, so we cannot assume it is equivalent to the
  // first index of the next block
  // here we use the last index of the block to enforce reflush later
  uint16_t capacity = state.cursor.idx.get_capacity();
  if (unlikely(state.cursor.idx.local_idx >= capacity))
    state.cursor.idx.local_idx = static_cast<uint16_t>(capacity - 1);
  meta->set_tx_tail(state.cursor.idx);
  return 0;
}

void File::stat(struct stat* buf) {
  buf->st_size = static_cast<off_t>(blk_table.update());
}

/*
 * Getters & removers for thread-local data structures
 */

Allocator* File::get_local_allocator() {
  if (auto it = allocators.find(tid); it != allocators.end()) {
    return &it->second;
  }

  auto [it, ok] = allocators.emplace(
      std::piecewise_construct, std::forward_as_tuple(tid),
      std::forward_as_tuple(&mem_table, &bitmap_mgr,
                            shm_mgr.alloc_per_thread_data()));
  PANIC_IF(!ok, "insert to thread-local allocators failed");
  return &it->second;
}

/*
 * Helper functions
 */

std::ostream& operator<<(std::ostream& out, const File& f) {
  __msan_scoped_disable_interceptor_checks();
  out << "File: fd = " << f.fd << "\n";
  if (f.can_write) out << f.shm_mgr;
  out << *f.meta;
  out << f.blk_table;
  out << f.mem_table;
  if (f.can_write) {
    out << f.bitmap_mgr;
  }
  out << f.tx_mgr;
  out << "\n";
  __msan_scoped_enable_interceptor_checks();

  return out;
}

}  // namespace ulayfs::dram
