#include "file.h"

namespace ulayfs::dram {

int File::open(const char* pathname, int flags, mode_t mode) {
  int ret;
  fd = posix::open(pathname, flags, mode);
  if (fd < 0) return fd;  // fail to open the file
  open_flags = flags;

  struct stat stat_buf;
  ret = posix::fstat(fd, &stat_buf);
  if (ret) throw std::runtime_error("Fail to fstat!");

  bool should_create = stat_buf.st_size == 0;
  if (should_create) {
    ret = posix::ftruncate(fd, LayoutOptions::prealloc_size);
    if (ret) throw std::runtime_error("Fail to ftruncate!");

    // call fstat again to get the latest file size
    ret = posix::fstat(fd, &stat_buf);
    if (ret) throw std::runtime_error("Fail to fstat!");
  }

  meta = mtable.init(fd, stat_buf.st_size);
  allocator.init(fd, meta, &mtable);

  if (should_create) meta->init();
  return fd;
}

};  // namespace ulayfs::dram
