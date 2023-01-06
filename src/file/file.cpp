#include "file.h"

#include <cmath>

#include "cursor/tx_block.h"

namespace ulayfs::dram {

File::File(int fd, const struct stat& stat, int flags,
           const char* pathname [[maybe_unused]])
    : mem_table(fd, stat.st_size, (flags & O_ACCMODE) == O_RDONLY),
      offset_mgr(),
      blk_table(&mem_table),
      shm_mgr(fd, stat, mem_table.get_meta()),
      meta(mem_table.get_meta()),
      fd(fd),
      can_read((flags & O_ACCMODE) == O_RDONLY ||
               (flags & O_ACCMODE) == O_RDWR),
      can_write((flags & O_ACCMODE) == O_WRONLY ||
                (flags & O_ACCMODE) == O_RDWR) {
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
        file_size = blk_table.update_unsafe(/*allocator=*/nullptr, &bitmap_mgr);
        file_size_updated = true;
        bitmap_mgr.entries[0].set_allocated(0);
      }
      meta->unlock();
    }
  }

  if (!file_size_updated) file_size = blk_table.update_unsafe();

  if (flags & O_APPEND) offset_mgr.seek_absolute(static_cast<off_t>(file_size));
  if constexpr (BuildOptions::debug) {
    path = strdup(pathname);
  }
}

File::~File() {
  allocators.clear();
  if (fd >= 0) posix::close(fd);
  if constexpr (BuildOptions::debug) {
    free((void*)path);
  }
}

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

std::ostream& operator<<(std::ostream& out, File& f) {
  __msan_scoped_disable_interceptor_checks();
  out << "File: fd = " << f.fd << "\n";
  if (f.can_write) out << f.shm_mgr;
  out << *f.meta;
  out << f.blk_table;
  out << f.mem_table;
  if (f.can_write) {
    out << f.bitmap_mgr;
  }
  out << f.offset_mgr;
  {
    out << "Transactions: \n";

    TxCursor cursor = TxCursor::from_meta(f.meta);
    int count = 0;

    while (true) {
      auto tx_entry = cursor.get_entry();
      if (!tx_entry.is_valid()) break;
      if (tx_entry.is_dummy()) goto next;

      count++;
      if (count > 10) {
        if (count % static_cast<int>(exp10(floor(log10(count)))) != 0)
          goto next;
      }

      out << "\t" << count << ": " << cursor.idx << " -> " << tx_entry << "\n";

      // print log entries if the tx is not inlined
      if (!tx_entry.is_inline()) {
        LogCursor log_cursor(tx_entry.indirect_entry, &f.mem_table);
        do {
          out << "\t\t" << *log_cursor << "\n";
        } while (log_cursor.advance(&f.mem_table));
      }

    next:
      if (bool success = cursor.advance(&f.mem_table); !success) break;
    }

    out << "\ttotal number of tx: " << count++ << "\n";
  }

  {
    out << "Tx Blocks: \n";
    TxBlockCursor cursor(f.meta);
    while (cursor.advance_to_next_block(&f.mem_table)) {
      out << "\t" << cursor.idx << ": " << *cursor.block << "\n";
    }
  }

  {
    out << "Orphaned Tx Blocks: \n";
    TxBlockCursor cursor(f.meta);
    while (cursor.advance_to_next_orphan(&f.mem_table)) {
      out << "\t" << cursor.idx << ": " << *cursor.block << "\n";
    }
  }
  out << "\n";
  __msan_scoped_enable_interceptor_checks();

  return out;
}

}  // namespace ulayfs::dram
