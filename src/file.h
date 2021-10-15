#pragma once
#include <stdexcept>

#include "layout.h"
#include "posix.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  pmem::MetaBlock* meta_block;

  File() : fd(-1), meta_block(nullptr) {}

  // test if File is in a valid state
  explicit operator bool() const { return fd >= 0; }
  bool operator!() const { return fd < 0; }

  int open(const char* pathname, int flags, mode_t mode) {
    int ret;
    struct stat stat_buf {};
    fd = posix::open(pathname, flags, mode);
    if (fd < 0) return fd;  // fail to open the file

    ret = posix::fstat(fd, &stat_buf);
    if (ret) throw std::runtime_error("Fail to fstat!");
    if (stat_buf.st_size == 0) {
      // this is a newly created file; do layout initialization
      ret = posix::ftruncate(fd, LayoutOptions::prealloc_size);
      if (ret) throw std::runtime_error("Fail to ftruncate!");

      // TODO: acquire futex here
      meta_block = static_cast<pmem::MetaBlock*>(posix::mmap(
          nullptr, LayoutOptions::prealloc_size, PROT_READ | PROT_WRITE,
          MAP_SHARED | MAP_HUGETLB | MAP_HUGE_2MB, fd, 0));
      if (!meta_block) throw std::runtime_error("Fail to mmap!");
      meta_block->init();
    } else if (stat_buf.st_size & (pmem::BLOCK_SIZE - 1))
      throw std::runtime_error("Invalid layout!");
    return fd;
  }
};

}  // namespace ulayfs::dram