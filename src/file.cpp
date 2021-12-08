#include "file.h"

namespace ulayfs::dram {

thread_local std::unordered_map<int, Allocator> File::allocators;
thread_local std::unordered_map<int, LogMgr> File::log_mgrs;

File::File(int fd, const struct stat* stat)
    : fd(fd),
      mem_table(fd, stat->st_size),
      meta(mem_table.get_meta()),
      tx_mgr(this, meta),
      blk_table(this, &tx_mgr),
      file_offset(0) {
  if (stat->st_size == 0) meta->init();

  meta->lock();
  shm_fd = open_shm(stat, bitmap);
  // The first bit corresponds to the meta block which should always be set
  // to 1. If it is not, then bitmap needs to be initialized.
  if ((bitmap[0].get() & 1) == 0) {
    TxEntryIdx tail_tx_idx;
    pmem::TxBlock* tail_tx_block;
    blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ false, true);
    // mark meta block as allocated
    bitmap[0].set_allocated(0);
  }
  meta->unlock();
}

File::~File() {
  DEBUG("~File called for fd=%d", fd);
  allocators.erase(fd);
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
  // currently, we always move the offset first so that we could pass the append
  // test
  off_t old_offset = __atomic_fetch_add(&file_offset, static_cast<off_t>(count),
                                        __ATOMIC_ACQ_REL);
  ssize_t ret = pwrite(buf, count, old_offset);
  return ret;
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

int File::open_shm(const struct stat* stat, Bitmap*& bitmap) {
  // TODO: enable dynamically grow bitmap
  char* shm_path = meta->get_shm_path_ref();
  // fill meta's shm_path in if it is empty
  if (*shm_path == '\0')
    sprintf(shm_path, "/dev/shm/ulayfs_%ld%ld%ld", stat->st_ino,
            stat->st_ctim.tv_sec, stat->st_ctim.tv_nsec);
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
    if (fchmod(shm_fd, stat->st_mode) < 0) {
      posix::close(shm_fd);
      PANIC("Fd \"%d\": fchmod on shared memory failed: %m", fd);
    }
    if (fchown(shm_fd, stat->st_uid, stat->st_gid) < 0) {
      posix::close(shm_fd);
      PANIC("Fd \"%d\": fchown on shared memory failed: %m", fd);
    }
    if (posix::fallocate(shm_fd, 0, 0, static_cast<off_t>(DRAM_BITMAP_SIZE)) <
        0) {
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
  void* shm = posix::mmap(nullptr, DRAM_BITMAP_SIZE, PROT_READ | PROT_WRITE,
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
