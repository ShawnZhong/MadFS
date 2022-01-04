#include "file.h"

#include <sys/xattr.h>

#include <cerrno>
#include <cstdio>
#include <iomanip>

#include "flock.h"
#include "idx.h"
#include "utils.h"

namespace ulayfs::dram {

File::File(int fd, const struct stat& stat, int flags, bool guard)
    : fd(fd),
      mem_table(fd, stat.st_size, (flags & O_ACCMODE) == O_RDONLY),
      meta(mem_table.get_meta()),
      tx_mgr(this, meta),
      log_mgr(this),
      blk_table(this, &tx_mgr),
      offset_mgr(this),
      can_read((flags & O_ACCMODE) == O_RDONLY ||
               (flags & O_ACCMODE) == O_RDWR),
      can_write((flags & O_ACCMODE) == O_WRONLY ||
                (flags & O_ACCMODE) == O_RDWR) {
  // lock the file to prevent gc before proceeding
  // the lock will be released only at close
  if (guard) flock::flock_guard(fd);

  pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
  if (stat.st_size == 0) meta->init();

  ssize_t rc = fgetxattr(fd, SHM_XATTR_NAME, &shm_path, sizeof(shm_path));
  if (rc == -1 && errno == ENODATA) {  // no shm_path attribute, create one
    sprintf(shm_path, "/dev/shm/ulayfs_%016lx_%013lx", stat.st_ino,
            (stat.st_ctim.tv_sec * 1000000000 + stat.st_ctim.tv_nsec) >> 3);
    rc = fsetxattr(fd, SHM_XATTR_NAME, shm_path, sizeof(shm_path), 0);
    PANIC_IF(rc == -1, "failed to set shm_path attribute");
  } else if (rc == -1) {
    PANIC("failed to get shm_path attribute");
  }

  open_shm(stat);

  // The first bit corresponds to the meta block which should always be set
  // to 1. If it is not, then bitmap needs to be initialized.
  // Bitmap::get is not thread safe but we are only reading one bit here.
  uint64_t file_size;
  bool file_size_updated = false;
  if (!bitmap[0].is_allocated(0)) {
    meta->lock();
    if (!bitmap[0].is_allocated(0)) {
      file_size = blk_table.get_file_size(/*init_bitmap*/ true);
      file_size_updated = true;
      bitmap[0].set_allocated(0);
    }
    meta->unlock();
  }
  if (!file_size_updated)
    file_size = blk_table.get_file_size(/*init_bitmap*/ false);

  if (flags & O_APPEND) offset_mgr.seek_absolute(file_size);
}

File::~File() {
  pthread_spin_destroy(&spinlock);
  allocators.clear();
  if (likely(is_fd_owned)) posix::close(fd);
  posix::munmap(bitmap, BITMAP_SIZE);
  posix::close(shm_fd);
  DEBUG("~File(): close(%d) and close(%d)", fd, shm_fd);
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

  pthread_spin_lock(&spinlock);
  uint64_t file_size = blk_table.get_file_size();

  switch (whence) {
    case SEEK_SET:
      ret = offset_mgr.seek_absolute(offset);
      break;
    case SEEK_CUR:
      ret = offset_mgr.seek_relative(offset);
      if (ret == -1) errno = EINVAL;
      break;
    case SEEK_END:
      ret = offset_mgr.seek_absolute(file_size + offset);
      break;
    // TODO: add SEEK_DATA and SEEK_HOLE
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
                 off_t offset) {
  if (offset % BLOCK_SIZE != 0) {
    errno = EINVAL;
    return MAP_FAILED;
  }

  // reserve address space by memory-mapping /dev/zero
  static int zero_fd = posix::open("/dev/zero", O_RDONLY);
  if (zero_fd == -1) {
    WARN("open(/dev/zero) failed");
    return MAP_FAILED;
  }
  void* res = posix::mmap(addr_hint, length, prot, mmap_flags, zero_fd, 0);
  if (res == MAP_FAILED) {
    WARN("mmap failed: %m");
    return MAP_FAILED;
  }
  char* new_addr = reinterpret_cast<char*>(res);
  char* old_addr = reinterpret_cast<char*>(meta);

  auto remap = [&old_addr, &new_addr](LogicalBlockIdx lidx,
                                      LogicalBlockIdx vidx, int num_blocks) {
    char* old_block_addr = old_addr + BLOCK_IDX_TO_SIZE(lidx);
    char* new_block_addr = new_addr + BLOCK_IDX_TO_SIZE(vidx);
    size_t len = BLOCK_IDX_TO_SIZE(num_blocks);
    int flag = MREMAP_MAYMOVE | MREMAP_FIXED;

    void* ret = posix::mremap(old_block_addr, len, len, flag, new_block_addr);
    return ret == new_block_addr;
  };

  // remap the blocks in the file
  VirtualBlockIdx vidx_end =
      BLOCK_SIZE_TO_IDX(ALIGN_UP(offset + length, BLOCK_SIZE));
  VirtualBlockIdx vidx_group_begin = BLOCK_SIZE_TO_IDX(offset);
  LogicalBlockIdx lidx_group_begin = blk_table.get(vidx_group_begin);
  int num_blocks = 0;
  for (size_t vidx = vidx_group_begin; vidx < vidx_end; vidx++) {
    LogicalBlockIdx lidx = blk_table.get(vidx);
    if (lidx == 0) PANIC("hole vidx=%ld in mmap", vidx);

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
  WARN("remap failed: %m");
  posix::munmap(new_addr, length);
  return MAP_FAILED;
}

int File::fsync() {
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  blk_table.update(&tail_tx_idx, &tail_tx_block);
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

void File::stat(struct stat* buf) {
  buf->st_size = static_cast<off_t>(blk_table.get_file_size());
}

/*
 * Getters & removers for thread-local data structures
 */

Allocator* File::get_local_allocator() {
  if (auto it = allocators.find(tid); it != allocators.end()) {
    return &it->second;
  }

  auto [it, ok] = allocators.emplace(tid, Allocator(this, bitmap));
  PANIC_IF(!ok, "insert to thread-local allocators failed");
  return &it->second;
}

/*
 * Helper functions
 */

void File::open_shm(const struct stat& stat) {
  // use posix::open instead of shm_open since shm_open calls open, which is
  // overloaded by ulayfs
  shm_fd =
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
      PANIC("create the temporary file failed");
    }

    // change permission and ownership of the new shared memory
    if (fchmod(shm_fd, stat.st_mode) < 0) {
      posix::close(shm_fd);
      PANIC("fchmod on shared memory failed");
    }
    if (fchown(shm_fd, stat.st_uid, stat.st_gid) < 0) {
      posix::close(shm_fd);
      PANIC("fchown on shared memory failed");
    }
    // TODO: enable dynamically grow bitmap
    if (posix::fallocate(shm_fd, 0, 0, static_cast<off_t>(BITMAP_SIZE)) < 0) {
      posix::close(shm_fd);
      PANIC("fallocate on shared memory failed");
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
        PANIC("cannot open or create the shared memory object");
      }
    }
  }

  DEBUG("posix::open(%s) = %d", shm_path, shm_fd);

  // mmap bitmap
  void* shm = posix::mmap(nullptr, BITMAP_SIZE, PROT_READ | PROT_WRITE,
                          MAP_SHARED, shm_fd, 0);
  if (shm == MAP_FAILED) {
    posix::close(shm_fd);
    PANIC("mmap bitmap failed");
  }

  bitmap = static_cast<Bitmap*>(shm);
}

void File::unlink_shm() {
  int ret = posix::unlink(shm_path);
  if (ret != 0) INFO("Fail to unlink bitmaps on shared memory: %m");
}

void File::tx_gc() {
  DEBUG("Garbage Collect for fd %d", fd);
  TxEntryIdx tail_tx_idx;
  uint64_t file_size;
  blk_table.update(&tail_tx_idx, /*tx_block*/ nullptr, &file_size);
  tx_mgr.gc(tail_tx_idx.block_idx, file_size);
}

std::ostream& operator<<(std::ostream& out, const File& f) {
  out << "File: fd = " << f.fd << "\n";
  out << "\tshm_path = " << f.shm_path << "\n";
  out << *f.meta;
  out << f.blk_table;
  out << f.mem_table;
  out << f.offset_mgr;
  out << "Bitmap: \n";
  for (size_t i = 0; i < f.meta->get_num_blocks() / BITMAP_CAPACITY; ++i) {
    out << "\t" << std::setw(6) << std::right << i * BITMAP_CAPACITY << " - "
        << std::setw(6) << std::left << (i + 1) * BITMAP_CAPACITY - 1 << ": "
        << f.bitmap[i] << "\n";
  }
  out << f.tx_mgr;
  out << "\n";

  return out;
}

}  // namespace ulayfs::dram
