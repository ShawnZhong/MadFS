#include "lib.h"

#include <tbb/concurrent_unordered_map.h>

#include <cstdarg>
#include <cstdio>
#include <unordered_map>

#include "config.h"
#include "posix.h"

namespace ulayfs {

// mapping between fd and in-memory file handle
// shared across threads within the same process
tbb::concurrent_unordered_map<int, std::shared_ptr<dram::File>> files;

std::shared_ptr<dram::File> get_file(int fd) {
  auto it = files.find(fd);
  if (it != files.end()) return it->second;
  return {};
}

extern "C" {
int open(const char* pathname, int flags, ...) {
  // keep a record of the user's intented flags before we hijack it
  int user_flags = flags;
  mode_t mode = 0;

  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  // TODO: support read-only files
  // if ((flags & O_ACCMODE) == O_RDONLY) {
  //   WARN("File \"%s\" opened with O_RDONLY. Fallback to syscall.", pathname);
  //   int fd = posix::open(pathname, flags, mode);
  //   DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
  //   return fd;
  // }

  if ((flags & O_ACCMODE) == O_WRONLY) {
    INFO("File \"%s\" opened with O_WRONLY. Changed to O_RDWR.", pathname);
    flags &= ~O_WRONLY;
    flags |= O_RDWR;
  }

  int fd = posix::open(pathname, flags, mode);
  DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);

  if (unlikely(fd < 0)) {
    WARN("File \"%s\" posix::open failed: %m", pathname);
    return fd;
  }

  struct stat stat_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  int rc = posix::fstat(fd, &stat_buf);
  if (unlikely(rc < 0)) {
    WARN("File \"%s\" fstat fialed: %m. Fallback to syscall.", pathname);
    return fd;
  }

  // we don't handle non-normal file (e.g., socket, directory, block dev)
  if (unlikely(!S_ISREG(stat_buf.st_mode) && !S_ISLNK(stat_buf.st_mode))) {
    WARN("Non-normal file \"%s\". Fallback to syscall.", pathname);
    return fd;
  }

  if (!IS_ALIGNED(stat_buf.st_size, BLOCK_SIZE)) {
    WARN("File size not aligned for \"%s\". Fallback to syscall", pathname);
    return fd;
  }

  pmem::Bitmap* bitmap;
  int shm_fd = -1;
  // TODO: enable the lines below when shm is acutally in use
  // int shm_fd = open_shm(pathname, &stat_buf, bitmap);
  // if (shm_fd < 0) {
  //   WARN("Failed to open bitmap for \"%s\". Fallback to syscall", pathname);
  //   return fd;
  // }

  files.emplace(fd, std::make_shared<dram::File>(fd, stat_buf.st_size, bitmap,
                                                 shm_fd, user_flags));
  DEBUG("ulayfs::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
  return fd;
}

int close(int fd) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::close(%d)", fd);
    file->erase_local_allocator();
    file->erase_local_log_mgr();
    files.unsafe_erase(fd);
    return posix::close(fd);
  } else {
    DEBUG("posix::close(%d)", fd);
    return posix::close(fd);
  }
}

ssize_t write(int fd, const void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::write(%d, buf, %zu)", fd, count);
    return file->write(buf, count);
  } else {
    DEBUG("posix::write(%d, buf, %zu)", fd, count);
    return posix::write(fd, buf, count);
  }
}

ssize_t read(int fd, void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::read(%d, buf, %zu)", fd, count);
    return file->read(buf, count);
  } else {
    DEBUG("posix::read(%d, buf, %zu)", fd, count);
    return posix::read(fd, buf, count);
  }
}

off_t lseek(int fd, off_t offset, int whence) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::lseek(%d, %ld, %d)", fd, offset, whence);
    return file->lseek(offset, whence);
  } else {
    DEBUG("posix::lseek(%d, %ld, %d)", fd, offset, whence);
    return posix::lseek(fd, offset, whence);
  }
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::pwrite(%d, buf, %zu, %ld)", fd, count, offset);
    return file->pwrite(buf, count, offset);
  } else {
    DEBUG("posix::pwrite(%d, buf, %zu, %ld)", fd, count, offset);
    return posix::pwrite(fd, buf, count, offset);
  }
}

ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::pread(%d, buf, %zu, %ld)", fd, count, offset);
    return file->pread(buf, count, offset);
  } else {
    DEBUG("posix::pread(%d, buf, %zu, %ld)", fd, count, offset);
    return posix::pread(fd, buf, count, offset);
  }
}

int fsync(int fd) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::fsync(%d)", fd);
    return file->fsync();
  } else {
    DEBUG("posix::fsync(%d)", fd);
    return posix::fsync(fd);
  }
}

int fstat(int fd, struct stat* buf) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::fstat(%d)", fd);
    // TODO: implement this
    return posix::fstat(fd, buf);
  } else {
    DEBUG("posix::fstat(%d)", fd);
    return posix::fstat(fd, buf);
  }
}

/**
 * Called when the shared library is first loaded
 *
 * Note that the global variables may not be initialized at this point
 * e.g., all the functions in the ulayfs::posix namespace
 */
void __attribute__((constructor)) ulayfs_ctor() {
  if (runtime_options.show_config) {
    std::cerr << build_options << std::endl;
    std::cerr << runtime_options << std::endl;
  }
  if (runtime_options.log_file) {
    log_file = fopen(runtime_options.log_file, "a");
  }
}

/**
 * Called when the shared library is unloaded
 */
void __attribute__((destructor)) ulayfs_dtor() { INFO("ulayfs_dtor called"); }
}  // extern "C"

/*
 * helper functions
 */

int open_shm(const char* pathname, const struct stat* stat,
             pmem::Bitmap*& bitmap) {
  // TODO: enable dynamically grow bitmap
  size_t shm_size = 8 * BLOCK_SIZE;
  char shm_path[PATH_MAX];
  sprintf(shm_path, "/dev/shm/ulayfs_%ld%ld%ld", stat->st_ino,
          stat->st_ctim.tv_sec, stat->st_ctim.tv_nsec);
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
      WARN("File \"%s\": create the temporary file failed: %m", pathname);
      return -1;
    }

    // change permission and ownership of the new shared memory
    if (fchmod(shm_fd, stat->st_mode) < 0) {
      WARN("File \"%s\": fchmod on shared memory failed: %m", pathname);
      posix::close(shm_fd);
      return -1;
    }
    if (fchown(shm_fd, stat->st_uid, stat->st_gid) < 0) {
      WARN("File \"%s\": fchown on shared memory failed: %m", pathname);
      posix::close(shm_fd);
      return -1;
    }
    if (posix::fallocate(shm_fd, 0, 0, static_cast<off_t>(shm_size)) < 0) {
      WARN("File \"%s\": fallocate on shared memory failed: %m", pathname);
      posix::close(shm_fd);
      return -1;
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
        WARN("File \"%s\" cannot open or create the shared memory object: %m",
             pathname);
        return -1;
      }
    }
  }

  // mmap bitmap
  void* shm = posix::mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                          shm_fd, 0);
  if (shm == MAP_FAILED) {
    WARN("File \"%s\" mmap bitmap failed: %m", pathname);
    posix::close(shm_fd);
    return -1;
  }

  bitmap = static_cast<pmem::Bitmap*>(shm);
  return shm_fd;
}
}  // namespace ulayfs
