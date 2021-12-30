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
  if (fd < 0) return {};
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

  try {
    files.emplace(fd, std::make_shared<dram::File>(fd, stat_buf, user_flags));
    INFO("ulayfs::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
  } catch (const dram::File::InitException& e) {
    WARN("File \"%s\": ulayfs::open failed: %s. Fallback to syscall", pathname,
         e.what());
  }

  return fd;
}

int close(int fd) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::close(%d)", fd);
    files.unsafe_erase(fd);
    return 0;
  } else {
    DEBUG("posix::close(%d)", fd);
    return posix::close(fd);
  }
}

int fclose(FILE* stream) {
  int fd = fileno(stream);
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::fclose(%d)", fd);
    files.unsafe_erase(fd);
    return 0;
  } else {
    DEBUG("posix::fclose(%d)", fd);
    return posix::fclose(stream);
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

void* mmap(void* addr, size_t length, int prot, int flags, int fd,
           off_t offset) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::mmap(%p, %zu, %x, %x, %d, %ld)", addr, length, prot, flags,
          fd, offset);
    return file->mmap(addr, length, prot, flags, offset);
  } else {
    DEBUG("posix::mmap(%p, %zu, %x, %x, %d, %ld)", addr, length, prot, flags,
          fd, offset);
    return posix::mmap(addr, length, prot, flags, fd, offset);
  }
}

int munmap(void* addr, size_t length) {
  DEBUG("posix::munmap(%p, %zu)", addr, length);
  return posix::munmap(addr, length);
}

int fstat(int fd, struct stat* buf) {
  int rc = posix::fstat(fd, buf);
  if (unlikely(rc < 0)) {
    WARN("fstat failed for fd = %d: %m", fd);
    return rc;
  }

  if (auto file = get_file(fd)) {
    file->stat(buf);
    DEBUG("ulayfs::fstat(%d)", fd);
  } else {
    DEBUG("posix::fstat(%d)", fd);
  }

  return 0;
}

int stat(const char* pathname, struct stat* buf) {
  int fd = open(pathname, O_RDONLY);
  if (unlikely(fd < 0)) {
    WARN("Could not open file \"%s\" for stat: %m", pathname);
    return -1;
  }

  int rc = posix::fstat(fd, buf);
  if (unlikely(rc < 0)) {
    WARN("stat failed for pathname = %s: %m", pathname);
    return rc;
  }

  if (auto file = get_file(fd)) {
    file->stat(buf);
    DEBUG("ulayfs::stat(%s)", pathname);
  } else {
    DEBUG("posix::stat(%s)", pathname);
  }

  return 0;
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
}  // namespace ulayfs
