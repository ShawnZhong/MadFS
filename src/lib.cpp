#include "lib.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <tbb/concurrent_unordered_map.h>
#include <unistd.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <iostream>

#include "config.h"
#include "const.h"
#include "file.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs {

static bool initialized = false;

// mapping between fd and in-memory file handle
// shared across threads within the same process
static tbb::concurrent_unordered_map<int, std::shared_ptr<dram::File>> files;

std::shared_ptr<dram::File> get_file(int fd) {
  if (!initialized) return {};
  if (fd < 0) return {};
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

  int fd;
  struct stat stat_buf;
  bool is_valid = dram::File::try_open(fd, stat_buf, pathname, flags, mode);
  if (!is_valid) {
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  try {
    files.emplace(fd,
                  std::make_shared<dram::File>(fd, stat_buf, flags, pathname));
    INFO("ulayfs::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    debug::counter.count("ulayfs::open");
  } catch (const FileInitException& e) {
    WARN("File \"%s\": ulayfs::open failed: %s. Fallback to syscall", pathname,
         e.what());
    DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
  } catch (const FatalException& e) {
    WARN("File \"%s\": ulayfs::open failed with fatal error.", pathname);
    return -1;
  }
  return fd;
}

int open64(const char* pathname, int flags, ...) {
  mode_t mode = 0;
  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  return open(pathname, flags, mode);
}

int openat64([[maybe_unused]] int dirfd, const char* pathname, int flags, ...) {
  mode_t mode = 0;
  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  // TODO: implement the case where pathname is relative to dirfd
  return open(pathname, flags, mode);
}

int close(int fd) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::close(%s)", file->path);
    files.unsafe_erase(fd);
    debug::counter.count("ulayfs::close");
    return 0;
  } else {
    DEBUG("posix::close(%d)", fd);
    return posix::close(fd);
  }
}

FILE *fopen(const char *filename, const char *mode){
  static INIT_FN(fopen);

  FILE* file = fopen(filename, mode);
  DEBUG("posix::fopen(%s, %s) = %p", filename, mode, file);
  return file;
}

int fclose(FILE* stream) {
  static INIT_FN(fclose);

  int fd = fileno(stream);
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::fclose(%s)", file->path);
    files.unsafe_erase(fd);
    debug::counter.count("ulayfs::fclose");
    return 0;
  } else {
    DEBUG("posix::fclose(%p)", stream);
    return fclose(stream);
  }
}

ssize_t read(int fd, void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    auto res = file->read(buf, count);
    DEBUG("ulayfs::read(%s, buf, %zu) = %zu", file->path, count, res);
    debug::counter.count("ulayfs::read", count);
    return res;
  } else {
    auto res = posix::read(fd, buf, count);
    DEBUG("posix::read(%d, buf, %zu) = %zu", fd, count, res);
    return res;
  }
}

ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    auto res = file->pread(buf, count, offset);
    DEBUG("ulayfs::pread(%s, buf, %zu, %ld) = %zu", file->path, count, offset,
          res);
    debug::counter.count("ulayfs::pread", count);
    return res;
  } else {
    auto res = posix::pread(fd, buf, count, offset);
    DEBUG("posix::pread(%d, buf, %zu, %ld) = %zu", fd, count, offset, res);
    return res;
  }
}

ssize_t pread64(int fd, void* buf, size_t count, off64_t offset) {
  return pread(fd, buf, count, offset);
}

ssize_t __read_chk(int fd, void* buf, size_t count,
                   [[maybe_unused]] size_t buflen) {
  assert(buflen >= count);
  return read(fd, buf, count);
}

ssize_t __pread_chk(int fd, void* buf, size_t count, off_t offset,
                    [[maybe_unused]] size_t buflen) {
  assert(buflen >= count);
  return pread(fd, buf, count, offset);
}

ssize_t write(int fd, const void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    ssize_t res = file->write(buf, count);
    DEBUG("ulayfs::write(%s, buf, %zu) = %zu", file->path, count, res);
    debug::counter.count("ulayfs::write", count);
    return res;
  } else {
    ssize_t res = posix::write(fd, buf, count);
    DEBUG("posix::write(%d, buf, %zu) = %zu", fd, count, res);
    return res;
  }
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::pwrite(%s, buf, %zu, %ld)", file->path, count, offset);
    debug::counter.count("ulayfs::pwrite", count);
    return file->pwrite(buf, count, offset);
  } else {
    DEBUG("posix::pwrite(%d, buf, %zu, %ld)", fd, count, offset);
    return posix::pwrite(fd, buf, count, offset);
  }
}

ssize_t pwrite64(int fd, const void* buf, size_t count, off64_t offset) {
  return pwrite(fd, buf, count, offset);
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

off64_t lseek64(int fd, off64_t offset, int whence) {
  return lseek(fd, offset, whence);
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

int fdatasync(int fd) {
  if (auto file = get_file(fd)) {
    DEBUG("ulayfs::fdatasync(%s)", file->path);
    return file->fsync();
  } else {
    DEBUG("posix::fdatasync(%d)", fd);
    return posix::fdatasync(fd);
  }
}
void* mmap(void* addr, size_t length, int prot, int flags, int fd,
           off_t offset) {
  if (auto file = get_file(fd)) {
    void* ret = file->mmap(addr, length, prot, flags, offset);
    DEBUG("ulayfs::mmap(%p, %zu, %x, %x, %d, %ld) = %p", addr, length, prot,
          flags, fd, offset, ret);
    return ret;
  } else {
    void* ret = posix::mmap(addr, length, prot, flags, fd, offset);
    DEBUG("posix::mmap(%p, %zu, %x, %x, %d, %ld) = %p", addr, length, prot,
          flags, fd, offset, ret);
    return ret;
  }
}

void* mmap64(void* addr, size_t length, int prot, int flags, int fd,
             off64_t offset) {
  return mmap(addr, length, prot, flags, fd, offset);
}

int __fxstat([[maybe_unused]] int ver, int fd, struct stat* buf) {
  int rc = posix::fstat(fd, buf);
  if (unlikely(rc < 0)) {
    WARN("fstat failed for fd = %d: %m", fd);
    return rc;
  }

  if (auto file = get_file(fd)) {
    file->stat(buf);
    DEBUG("ulayfs::fstat(%d, {.st_size = %ld})", fd, buf->st_size);
  } else {
    DEBUG("posix::fstat(%d)", fd);
  }

  return 0;
}

int __fxstat64([[maybe_unused]] int ver, int fd, struct stat64* buf) {
  return __fxstat(ver, fd, reinterpret_cast<struct stat*>(buf));
}

int __xstat([[maybe_unused]] int ver, const char* pathname, struct stat* buf) {
  if (int rc = posix::stat(pathname, buf); unlikely(rc < 0)) {
    WARN("stat failed for pathname = %s: %m", pathname);
    return rc;
  }

  if (ssize_t rc = getxattr(pathname, SHM_XATTR_NAME, nullptr, 0); rc > 0) {
    int fd = ulayfs::open(pathname, O_RDONLY);
    get_file(fd)->stat(buf);
    WARN("ulayfs::stat(%s) = {.st_size = %ld}", pathname, buf->st_size);
    ulayfs::close(fd);
    DEBUG("ulayfs::stat(%s)", pathname);
  } else {
    DEBUG("posix::stat(%s)", pathname);
  }

  return 0;
}

int __xstat64([[maybe_unused]] int ver, const char* pathname,
              struct stat64* buf) {
  return __xstat(ver, pathname, reinterpret_cast<struct stat*>(buf));
}

int unlink(const char* path) {
  dram::File::unlink_shm(path);
  int rc = posix::unlink(path);
  DEBUG("posix::unlink(%s) = %d", path, rc);
  return rc;
}

int rename(const char* oldpath, const char* newpath) {
  if (access(newpath, F_OK) == 0) dram::File::unlink_shm(newpath);
  int rc = posix::rename(oldpath, newpath);
  DEBUG("posix::rename(%s, %s) = %d", oldpath, newpath, rc);
  return rc;
}

int truncate([[maybe_unused]] const char* path, [[maybe_unused]] off_t length) {
  PANIC("truncate not implemented");
  return -1;
}

int ftruncate([[maybe_unused]] int fd, [[maybe_unused]] off_t length) {
  PANIC("ftruncate not implemented");
  return -1;
}

int flock([[maybe_unused]] int fd, [[maybe_unused]] int operation) {
  PANIC("flock not implemented");
  return -1;
}

int fcntl(int fd, int cmd, ... /* arg */) {
  return 0;
  va_list arg;
  va_start(arg, cmd);
  auto res = posix::fcntl(fd, cmd, arg);
  va_end(arg);
  DEBUG("posix::fcntl(%d, %d, ...) = %d", fd, cmd, res);
  return res;
}

int fcntl64(int fd, int cmd, ... /* arg */) {
  return 0;
  va_list arg;
  va_start(arg, cmd);
  auto res = posix::fcntl(fd, cmd, arg);
  va_end(arg);
  DEBUG("posix::fcntl(%d, %d, ...) = %d", fd, cmd, res);
  return res;
}

/**
 * Called when the shared library is first loaded
 *
 * Note that the global variables may not be initialized at this point
 * e.g., all the functions in the ulayfs::posix namespace
 */
void __attribute__((constructor)) ulayfs_ctor() {
  initialized = true;
  std::cerr << build_options << std::endl;
  std::cerr << runtime_options << std::endl;
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
