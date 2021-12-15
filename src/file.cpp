#include "file.h"

namespace ulayfs::dram {

File::File(int fd, const struct stat& stat, int flags)
    : fd(fd),
      mem_table(fd, stat.st_size, (flags & O_ACCMODE) == O_RDONLY),
      meta(mem_table.get_meta()),
      tx_mgr(this, meta),
      blk_table(this, &tx_mgr),
      file_offset(0),
      flags(flags) {
  if (stat.st_size == 0) meta->init();

  const char* shm_path = meta->get_shm_path();
  // fill meta's shm_path in if it is empty
  if (*shm_path == '\0') {
    meta->lock();
    if (*shm_path == '\0') meta->set_shm_path(stat);
    meta->unlock();
  }

  shm_fd = open_shm(shm_path, stat, bitmap);
  // The first bit corresponds to the meta block which should always be set
  // to 1. If it is not, then bitmap needs to be initialized.
  // Bitmap::get is not thread safe but we are only reading one bit here.
  if ((bitmap[0].get() & 1) == 0) {
    meta->lock();
    if ((bitmap[0].get() & 1) == 0) {
      TxEntryIdx tail_tx_idx;
      pmem::TxBlock* tail_tx_block;
      blk_table.update(tail_tx_idx, tail_tx_block, nullptr, /*do_alloc*/ false,
                       true);
      // mark meta block as allocated
      bitmap[0].set_allocated(0);
    }
    meta->unlock();
  }

  // FIXME: the file_offset operation must be thread-safe
  if ((flags & O_ACCMODE) == O_APPEND)
    file_offset += blk_table.get_file_size();
}

File::~File() {
  DEBUG("posix::close(%d)", fd);
  posix::close(fd);
  // TODO: enable the lines below when shm is acutally in use
  posix::close(shm_fd);
  allocators.clear();
  log_mgrs.clear();
}

File::~File() {
  DEBUG("~File called for fd=%d", fd);
  allocators.erase(fd);
}

/*
 * POSIX I/O operations
 */

ssize_t File::pwrite(const void* buf, size_t count, size_t offset) {
  if ((flags & O_ACCMODE) != O_WRONLY && (flags & O_ACCMODE) != O_RDWR) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  return tx_mgr.do_write(static_cast<const char*>(buf), count, offset);
}

ssize_t File::write(const void* buf, size_t count) {
  if ((flags & O_ACCMODE) != O_WRONLY && (flags & O_ACCMODE) != O_RDWR) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  // FIXME: offset upate must is not thread safe (must associated with tx
  // application on BlkTable)
  // currently, we always move the offset first so that we could pass the append
  // test
  off_t old_offset = __atomic_fetch_add(&file_offset, static_cast<off_t>(count),
                                        __ATOMIC_ACQ_REL);
  return tx_mgr.do_write(static_cast<const char*>(buf), count, old_offset);
}

ssize_t File::pread(void* buf, size_t count, off_t offset) {
  if ((flags & O_ACCMODE) != O_RDONLY && (flags & O_ACCMODE) != O_RDWR) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  return tx_mgr.do_read(static_cast<char*>(buf), count, offset);
}

ssize_t File::read(void* buf, size_t count) {
  if ((flags & O_ACCMODE) != O_RDONLY && (flags & O_ACCMODE) != O_RDWR) {
    errno = EBADF;
    return -1;
  }
  // FIXME: offset upate must is not thread safe (must associated with tx
  // application on BlkTable)
  ssize_t ret = pread(buf, count, file_offset);
  if (ret > 0)
    __atomic_fetch_add(&file_offset, static_cast<off_t>(ret), __ATOMIC_ACQ_REL);
  return ret;
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
 * Getters & removers for thread-local data structures
 */

Allocator* File::get_local_allocator() {
  if (auto it = allocators.find(tid); it != allocators.end()) {
    return &it->second;
  }

  auto [it, ok] =
      allocators.emplace(tid, Allocator(fd, meta, &mem_table, bitmap));
  PANIC_IF(!ok, "insert to thread-local allocators failed");
  return &it->second;
}

LogMgr* File::get_local_log_mgr() {
  if (auto it = log_mgrs.find(tid); it != log_mgrs.end()) {
    return &it->second;
  }

  auto [it, ok] =
      log_mgrs.emplace(tid, LogMgr(this, meta));
  PANIC_IF(!ok, "insert to thread-local log_mgrs failed");
  return &it->second;
}

/*
 * Helper functions
 */

int File::open_shm(const char* shm_path, const struct stat& stat,
                   Bitmap*& bitmap) {
  DEBUG("Opening shared memory %s", shm_path);
  // use posix::open instead of shm_open since shm_open calls open, which is
  // overloaded by ulayfs
  int shm_fd =
      posix::open(shm_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC, S_IRUSR | S_IWUSR);

  // if the file does not exist, create it
  if (shm_fd < 0) {
    // We create a temporary file first, and then use `linkat` to put the file
    // into the directory `/dev/shm`. This ensures the atomicity of the creating
    // the shared memory file and setting its permission.
    shm_fd =
        posix::open("/dev/shm", O_TMPFILE | O_RDWR | O_NOFOLLOW | O_CLOEXEC,
                    S_IRUSR | S_IWUSR);
    if (unlikely(shm_fd < 0)) {
      PANIC("Fd \"%d\": create the temporary file failed: %m", fd);
    }

    // change permission and ownership of the new shared memory
    if (fchmod(shm_fd, stat.st_mode) < 0) {
      posix::close(shm_fd);
      PANIC("Fd \"%d\": fchmod on shared memory failed: %m", fd);
    }
    if (fchown(shm_fd, stat.st_uid, stat.st_gid) < 0) {
      posix::close(shm_fd);
      PANIC("Fd \"%d\": fchown on shared memory failed: %m", fd);
    }
    // TODO: enable dynamically grow bitmap
    if (posix::fallocate(shm_fd, 0, 0, static_cast<off_t>(BITMAP_SIZE)) < 0) {
      posix::close(shm_fd);
      PANIC("Fd \"%d\": fallocate on shared memory failed: %m", fd);
    }

    // publish the created tmpfile.
    char tmpfile_path[PATH_MAX];
    sprintf(tmpfile_path, "/proc/self/fd/%d", shm_fd);
    int rc =
        linkat(AT_FDCWD, tmpfile_path, AT_FDCWD, shm_path, AT_SYMLINK_FOLLOW);
    if (rc < 0) {
      // Another process may have created a new shared memory before us. Retry
      // opening.
      posix::close(shm_fd);
      shm_fd = posix::open(shm_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC,
                           S_IRUSR | S_IWUSR);
      if (shm_fd < 0) {
        PANIC("Fd \"%d\" cannot open or create the shared memory object: %m",
              fd);
      }
    }
  }

  // mmap bitmap
  void* shm = posix::mmap(nullptr, BITMAP_SIZE, PROT_READ | PROT_WRITE,
                          MAP_SHARED, shm_fd, 0);
  if (shm == MAP_FAILED) {
    posix::close(shm_fd);
    PANIC("Fd \"%d\" mmap bitmap failed: %m", fd);
  }

  bitmap = static_cast<dram::Bitmap*>(shm);
  return shm_fd;
}

std::ostream& operator<<(std::ostream& out, const File& f) {
  out << "File: fd = " << f.fd << ", offset = " << f.file_offset << "\n";
  out << *f.meta;
  out << f.blk_table;
  out << f.mem_table;
  out << "Dram_bitmap: \n";
  for (size_t i = 0; i < f.meta->get_num_blocks() / 64; ++i) {
    out << "\t" << i * 64 << "-" << (i + 1) * 64 - 1 << ": " << f.bitmap[i]
        << "\n";
  }
  out << f.tx_mgr;
  out << "\n";

  return out;
}

}  // namespace ulayfs::dram
