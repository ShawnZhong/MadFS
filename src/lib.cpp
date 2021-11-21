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
  mode_t mode = 0;

  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  // TODO: support read-only files
  if ((flags & O_ACCMODE) == O_RDONLY) {
    WARN("File \"%s\" opened with O_RDONLY. Fallback to syscall.", pathname);
    int fd = posix::open(pathname, flags, mode);
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  if ((flags & O_ACCMODE) == O_WRONLY) {
    INFO("File \"%s\" opened with O_WRONLY. Changed to O_RDWR.", pathname);
    flags &= ~O_WRONLY;
    flags |= O_RDWR;
  }

  int fd = posix::open(pathname, flags, mode);

  if (fd < 0) {
    WARN("File \"%s\" posix::open failed: %m", pathname);
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  struct stat stat_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  int rc = posix::fstat(fd, &stat_buf);
  if (rc < 0) {
    WARN("File \"%s\" fstat fialed: %m. Fallback to syscall.", pathname);
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  // we don't handle non-normal file (e.g., socket, directory, block dev)
  if (!S_ISREG(stat_buf.st_mode) && !S_ISLNK(stat_buf.st_mode)) {
    WARN("Non-normal file \"%s\". Fallback to syscall.", pathname);
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  if (!IS_ALIGNED(stat_buf.st_size, BLOCK_SIZE)) {
    WARN("File size not aligned for \"%s\". Fallback to syscall", pathname);
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  files.emplace(fd, std::make_shared<dram::File>(fd, stat_buf.st_size));
  INFO("ulayfs::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
  return fd;
}

int close(int fd) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::close(%d)", fd);
    files.unsafe_erase(fd);
    return 0;
  } else {
    DEBUG("posix::close(%d)", fd);
    return posix::close(fd);
  }
}

ssize_t write(int fd, const void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::write(%d, buf, %zu)", fd, count);
    return file->write(buf, count);
  } else {
    DEBUG("posix::write(%d, buf, %zu)", fd, count);
    return posix::write(fd, buf, count);
  }
}

ssize_t read(int fd, void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::read(%d, buf, %zu)", fd, count);
    return file->read(buf, count);
  } else {
    DEBUG("posix::read(%d, buf, %zu)", fd, count);
    return posix::read(fd, buf, count);
  }
}

off_t lseek(int fd, off_t offset, int whence) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::lseek(%d, %ld, %d)", fd, offset, whence);
    return file->lseek(offset, whence);
  } else {
    DEBUG("posix::lseek(%d, %ld, %d)", fd, offset, whence);
    return posix::lseek(fd, offset, whence);
  }
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::pwrite(%d, buf, %zu, %ld)", fd, count, offset);
    return file->pwrite(buf, count, offset);
  } else {
    DEBUG("posix::pwrite(%d, buf, %zu, %ld)", fd, count, offset);
    return posix::pwrite(fd, buf, count, offset);
  }
}

ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::pread(%d, buf, %zu, %ld)", fd, count, offset);
    return file->pread(buf, count, offset);
  } else {
    DEBUG("posix::pread(%d, buf, %zu, %ld)", fd, count, offset);
    return posix::pread(fd, buf, count, offset);
  }
}

int fsync(int fd) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::fsync()");
    return file->fsync();
  } else {
    DEBUG("posix::fstat()");
    return posix::fsync(fd);
  }
}

int fstat(int fd, struct stat* buf) {
  if (auto file = get_file(fd)) {
    INFO("ulayfs::fstat(%d)", fd);
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
void __attribute__((destructor)) ulayfs_dtor() {}
}  // extern "C"
}  // namespace ulayfs
