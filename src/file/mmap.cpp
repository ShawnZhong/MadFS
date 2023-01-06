#include <sys/mman.h>

#include "file/file.h"

namespace ulayfs::dram {
void* File::mmap(void* addr_hint, size_t length, int prot, int mmap_flags,
                 size_t offset) const {
  if (offset % BLOCK_SIZE != 0) {
    errno = EINVAL;
    return MAP_FAILED;
  }

  // reserve address space by memory-mapping /dev/zero
  static int zero_fd = posix::open("/dev/zero", O_RDONLY);
  if (zero_fd == -1) {
    LOG_WARN("open(/dev/zero) failed");
    return MAP_FAILED;
  }
  void* res = posix::mmap(addr_hint, length, prot, mmap_flags, zero_fd, 0);
  if (res == MAP_FAILED) {
    LOG_WARN("mmap failed: %m");
    return MAP_FAILED;
  }
  char* new_addr = reinterpret_cast<char*>(res);
  char* old_addr = reinterpret_cast<char*>(meta);
  // TODO: there is a kernel bug that when the old_addr is unmapped, accessing
  //  new_addr results in kernel panic

  auto remap = [&old_addr, &new_addr](LogicalBlockIdx lidx,
                                      VirtualBlockIdx vidx,
                                      uint32_t num_blocks) {
    char* old_block_addr = old_addr + BLOCK_IDX_TO_SIZE(lidx);
    char* new_block_addr = new_addr + BLOCK_IDX_TO_SIZE(vidx);
    size_t len = BLOCK_NUM_TO_SIZE(num_blocks);
    int flag = MREMAP_MAYMOVE | MREMAP_FIXED;

    void* ret = posix::mremap(old_block_addr, len, len, flag, new_block_addr);
    return ret == new_block_addr;
  };

  // remap the blocks in the file
  VirtualBlockIdx vidx_end =
      BLOCK_SIZE_TO_IDX(ALIGN_UP(offset + length, BLOCK_SIZE));
  VirtualBlockIdx vidx_group_begin = BLOCK_SIZE_TO_IDX(offset);
  LogicalBlockIdx lidx_group_begin = blk_table.vidx_to_lidx(vidx_group_begin);
  uint32_t num_blocks = 0;
  for (VirtualBlockIdx vidx = vidx_group_begin; vidx < vidx_end; ++vidx) {
    LogicalBlockIdx lidx = blk_table.vidx_to_lidx(vidx);
    if (lidx == 0) PANIC("hole vidx=%d in mmap", vidx.get());

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
  LOG_WARN("remap failed: %m");
  posix::munmap(new_addr, length);
  return MAP_FAILED;
}
}  // namespace ulayfs::dram
