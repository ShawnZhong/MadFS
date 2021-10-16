#pragma once

#include <bits/stdint-uintn.h>

#include <stdexcept>

#include "config.h"
#include "file.h"
#include "layout.h"
#include "posix.h"

namespace ulayfs::dram {

class Allocator {
  File* file;
  Allocator(File* f) : file(f) {}

  // ask more blocks for the kernel filesystem, such that least_idx is valid
  void grow(pmem::BlockIdx least_idx) {
    int ret;
    if (least_idx < file->get_num_blocks()) return;
    file->lock();

    uint32_t new_num_blocks =
        ((least_idx >> LayoutOptions::grow_unit_shift) + 1)
        << LayoutOptions::grow_unit_shift;
    ret = posix::ftruncate(file->get_fd(), static_cast<long>(new_num_blocks)
                                               << pmem::BLOCK_SHIFT);
    if (ret) throw std::runtime_error("Fail to ftruncate!");

    file->unlock();
  }
};

};  // namespace ulayfs::dram
