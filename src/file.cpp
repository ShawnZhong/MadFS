#include "file.h"

namespace ulayfs::dram {

thread_local std::unordered_map<int, Allocator> File::allocators;
thread_local std::unordered_map<int, LogMgr> File::log_mgrs;

File::File(int fd, off_t init_file_size, pmem::Bitmap* bitmap, int shm_fd)
    : fd(fd),
      bitmap(bitmap),
      mem_table(fd, init_file_size),
      meta(mem_table.get_meta()),
      tx_mgr(this, meta, &mem_table),
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

  // TODO: support writing to offset beyond file_size. In POSIX standard
  // this is allowd and will create a hole for the gap region (where reads
  // return null bytes). Related to SEEK_HOLE and SEEK_DATA in lseek()
  if (offset > meta->get_file_size()) return 0;

  tx_mgr.do_write(static_cast<const char*>(buf), count, offset);
  // TODO: handle write fails i.e. return value != count
  return static_cast<ssize_t>(count);
}

ssize_t File::pread(void* buf, size_t count, off_t offset) {
  if (count == 0) return 0;

  // check against current file_size
  // if offset >= file_size, return zero to indicate EOF
  // if offset < file_size while offset + count > file_size, cut count to EOF
  off_t file_size = static_cast<off_t>(meta->get_file_size());
  if (offset >= file_size)
    return 0;
  else if (offset + static_cast<off_t>(count) > file_size)
    count = file_size - offset;

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
        new_off = meta->get_file_size() + offset;
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
  // atomically add then return the old value
  off_t old_off = __atomic_fetch_add(&file_offset, static_cast<off_t>(count),
                                     __ATOMIC_ACQ_REL);

  return pwrite(buf, count, old_off);
}

ssize_t File::read(void* buf, size_t count) {
  off_t new_off;
  off_t old_off = file_offset;

  // check against current file_size
  // if offset >= file_size, return zero to indicate EOF
  // if offset < file_size while offset + count > file_size, cut count to EOF
  off_t file_size = static_cast<off_t>(meta->get_file_size());
  if (old_off >= file_size)
    return 0;
  else if (old_off + static_cast<off_t>(count) > file_size)
    count = file_size - old_off;

  do {
    new_off = old_off + static_cast<off_t>(count);
  } while (!__atomic_compare_exchange_n(&file_offset, &old_off, new_off, false,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));

  return pread(buf, count, old_off);
}

int File::fsync() {
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ false);
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

  auto [it, ok] = log_mgrs.emplace(fd, LogMgr(this, meta, &mem_table));
  PANIC_IF(!ok, "insert to thread-local log_mgrs failed");
  return &it->second;
}

/*
 * Helper functions
 */

const pmem::Block* File::vidx_to_addr_ro(VirtualBlockIdx vidx) {
  static const char empty_block[BLOCK_SIZE]{};

  LogicalBlockIdx lidx = blk_table.get(vidx);
  if (lidx == 0) return reinterpret_cast<const pmem::Block*>(&empty_block);
  return mem_table.get(lidx);
}

pmem::Block* File::vidx_to_addr_rw(VirtualBlockIdx vidx) {
  return mem_table.get(blk_table.get(vidx));
}

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
