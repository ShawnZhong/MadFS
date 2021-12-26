#include "file.h"

namespace ulayfs::dram {

File::File(int fd, const struct stat& stat, int flags)
    : fd(fd),
      mem_table(fd, stat.st_size, (flags & O_ACCMODE) == O_RDONLY),
      meta(mem_table.get_meta()),
      tx_mgr(this, meta),
      blk_table(this, &tx_mgr),
      offset_mgr(this),
      flags(flags),
      can_read((flags & O_ACCMODE) == O_RDONLY ||
               (flags & O_ACCMODE) == O_RDWR),
      can_write((flags & O_ACCMODE) == O_RDONLY ||
                (flags & O_ACCMODE) == O_RDWR) {
  pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
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
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  uint64_t file_size;
  if ((bitmap[0].get() & 1) == 0) {
    meta->lock();
    if ((bitmap[0].get() & 1) == 0) {
      blk_table.update(tail_tx_idx, tail_tx_block, &file_size,
                       /*do_alloc*/ false, /*init_bitmap*/ true);
      // mark meta block as allocated
      bitmap[0].set_allocated(0);
    }
    meta->unlock();
  } else {
    // if bitmap has been set up, still apply tx to the tail so that
    // file_size is up-to-date
    blk_table.update(tail_tx_idx, tail_tx_block, &file_size,
                     /*do_alloc*/ false, /*init_bitmap*/ false);
  }

  if (flags & O_APPEND) offset_mgr.seek_absolute(file_size);
}

File::~File() {
  pthread_spin_destroy(&spinlock);
  DEBUG("posix::close(%d)", fd);
  posix::close(fd);
  posix::close(shm_fd);
  allocators.clear();
  log_mgrs.clear();
}

/*
 * POSIX I/O operations
 */

ssize_t File::pwrite(const void* buf, size_t count, size_t offset) {
  if (!can_write) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  return tx_mgr.do_pwrite(static_cast<const char*>(buf), count, offset);
}

ssize_t File::write(const void* buf, size_t count) {
  if (!can_write) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  return tx_mgr.do_write(static_cast<const char*>(buf), count);
}

ssize_t File::pread(void* buf, size_t count, off_t offset) {
  if (!can_read) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  return tx_mgr.do_pread(static_cast<char*>(buf), count, offset);
}

ssize_t File::read(void* buf, size_t count) {
  if (!can_read) {
    errno = EBADF;
    return -1;
  }
  if (count == 0) return 0;
  return tx_mgr.do_read(static_cast<char*>(buf), count);
}

off_t File::lseek(off_t offset, int whence) {
  int64_t ret;
  TxEntryIdx tx_idx;
  pmem::TxBlock* tx_block;
  uint64_t file_size;

  pthread_spin_lock(&spinlock);
  blk_table.update(tx_idx, tx_block, &file_size, /*do_alloc*/ false);

  switch (whence) {
    case SEEK_SET:
      ret = offset_mgr.seek_absolute(offset);
      break;
    case SEEK_CUR:
      ret = offset_mgr.seek_relative(offset);
      break;
    case SEEK_END:
      ret = offset_mgr.seek_absolute(file_size + offset);
      break;
    // TODO: add SEEK_DATA and SEEK_HOLE
    case SEEK_DATA:
    case SEEK_HOLE:
    default:
      ret = -1;
  }

  pthread_spin_unlock(&spinlock);
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

  auto [it, ok] = log_mgrs.emplace(tid, LogMgr(this, meta));
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
  out << "File: fd = " << f.fd << "\n";
  out << *f.meta;
  out << f.blk_table;
  out << f.mem_table;
  out << f.offset_mgr;
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
