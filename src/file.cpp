#include "file.h"

namespace ulayfs::dram {

thread_local std::unordered_map<int, Allocator> File::allocators;
thread_local std::unordered_map<int, LogMgr> File::log_mgrs;

File::File(int fd, off_t init_file_size, pmem::Bitmap* bitmap, int shm_fd)
    : fd(fd),
      bitmap(bitmap),
      mem_table(fd, init_file_size),
      meta(mem_table.get_meta()),
      tx_mgr(this, meta),
      blk_table(this, &tx_mgr),
      shm_fd(shm_fd),
      file_offset(0) {
  if (init_file_size == 0) meta->init();
}

/*
 * POSIX I/O operations
 */

ssize_t File::pwrite(const void* buf, size_t count, size_t offset) {
  if (count == 0) return 0;
  tx_mgr.do_write(static_cast<const char*>(buf), count, offset);
  // TODO: handle write fails i.e. return value != count
  return static_cast<ssize_t>(count);
}

ssize_t File::pread(void* buf, size_t count, off_t offset) {
  if (count == 0) return 0;
  return tx_mgr.do_read(static_cast<char*>(buf), count, offset);
}

off_t File::lseek(off_t offset, int whence) {
  off_t old_off = file_offset;
  off_t new_off;

  switch (whence) {
    case SEEK_SET: {
      if (offset < 0) return -1;
      __atomic_store_n(&file_offset, offset, __ATOMIC_RELEASE);
      return file_offset;
    }

    case SEEK_CUR: {
      do {
        new_off = old_off + offset;
        if (new_off < 0) return -1;
      } while (!__atomic_compare_exchange_n(&file_offset, &old_off, new_off,
                                            false, __ATOMIC_ACQ_REL,
                                            __ATOMIC_RELAXED));
      return file_offset;
    }

    case SEEK_END: {
      do {
        // FIXME: file offset and file size are not thread-safe to read directly
        new_off = blk_table.get_file_size() + offset;
        if (new_off < 0) return -1;
      } while (!__atomic_compare_exchange_n(&file_offset, &old_off, new_off,
                                            false, __ATOMIC_ACQ_REL,
                                            __ATOMIC_RELAXED));
      return file_offset;
    }

    // TODO: add SEEK_DATA and SEEK_HOLE
    case SEEK_DATA:
    case SEEK_HOLE:
    default:
      return -1;
  }
}

ssize_t File::write(const void* buf, size_t count) {
  // FIXME: offset upate must is not thread safe (must associated with tx
  // application on BlkTable)
  ssize_t ret = pwrite(buf, count, file_offset);
  if (ret > 0)
    __atomic_fetch_add(&file_offset, static_cast<off_t>(ret), __ATOMIC_ACQ_REL);
  return pwrite(buf, count, file_offset);
}

ssize_t File::read(void* buf, size_t count) {
  // FIXME: offset upate must is not thread safe (must associated with tx
  // application on BlkTable)
  ssize_t ret = pread(buf, count, file_offset);
  if (ret > 0)
    __atomic_fetch_add(&file_offset, static_cast<off_t>(ret), __ATOMIC_ACQ_REL);
  return ret;
}

int File::fsync() {
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  blk_table.update(tail_tx_idx, tail_tx_block, nullptr, /*do_alloc*/ false);
  tx_mgr.flush_tx_entries(meta->get_tx_tail(), tail_tx_idx, tail_tx_block);
  // we keep an invariant that tx_tail must be a valid (non-overflow) idx
  // an overflow index implies that the `next` pointer of the block is not set
  // (and thus not flushed) yet, so we cannot assume it is equivalent to the
  // first index of the next block
  // here we use the last index of the block to enforce reflush later
  uint16_t capacity =
      tail_tx_idx.block_idx == 0 ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
  if (unlikely(tail_tx_idx.local_idx >= capacity))
    tail_tx_idx.local_idx = capacity - 1;
  meta->set_tx_tail(tail_tx_idx);
  return 0;
}

/*
 * Getters for thread-local data structures
 */

Allocator* File::get_local_allocator() {
  if (auto it = allocators.find(fd); it != allocators.end()) {
    return &it->second;
  }

  auto [it, ok] =
      allocators.emplace(fd, Allocator(fd, meta, &mem_table, bitmap));
  PANIC_IF(!ok, "insert to thread-local allocators failed");
  return &it->second;
}

LogMgr* File::get_local_log_mgr() {
  if (auto it = log_mgrs.find(fd); it != log_mgrs.end()) {
    return &it->second;
  }

  auto [it, ok] = log_mgrs.emplace(fd, LogMgr(this, meta));
  PANIC_IF(!ok, "insert to thread-local log_mgrs failed");
  return &it->second;
}

/*
 * Helper functions
 */

std::ostream& operator<<(std::ostream& out, const File& f) {
  out << "File: fd = " << f.fd << ", offset = " << f.file_offset << "\n";
  out << *f.meta;
  out << f.mem_table;
  out << f.tx_mgr;
  out << f.blk_table;
  out << "\n";

  return out;
}

}  // namespace ulayfs::dram
