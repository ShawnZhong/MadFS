#include "file.h"

#include <linux/mman.h>

namespace ulayfs::dram {

int File::open(const char* pathname, int flags, mode_t mode) {
  int ret;
  bool is_create = false;
  fd = posix::open(pathname, flags, mode);
  if (fd < 0) return fd;  // fail to open the file
  open_flags = flags;

  if (flags & O_CREAT) {
    struct stat stat_buf;
    ret = posix::fstat(fd, &stat_buf);
    if (ret) throw std::runtime_error("Fail to fstat!");
    bool is_create = stat_buf.st_size == 0;
  }

  if (is_create) {
    ret = posix::ftruncate(fd, LayoutOptions::prealloc_size);
    if (ret) throw std::runtime_error("Fail to ftruncate!");
  }

  meta_block = static_cast<pmem::MetaBlock*>(
      posix::mmap(nullptr, LayoutOptions::prealloc_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_HUGETLB | MAP_HUGE_2MB, fd, 0));
  if (!meta_block) throw std::runtime_error("Fail to mmap!");

  if (is_create)
    meta_block->init();
  else
    meta_block->verify_ready();
  return fd;
}

};  // namespace ulayfs::dram
