#include "file.h"

#include "block.h"

namespace ulayfs::dram {

int File::open(const char* pathname, int flags, mode_t mode) {
  int ret;
  fd = posix::open(pathname, flags, mode);
  if (fd < 0) return fd;  // fail to open the file
  open_flags = flags;

  struct stat stat_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  ret = posix::fstat(fd, &stat_buf);
  panic_if(ret, "fstat failed");

  is_ulayfs_file = S_ISREG(stat_buf.st_mode) || S_ISLNK(stat_buf.st_mode);
  if (!is_ulayfs_file) return fd;

  if (!IS_ALIGNED(stat_buf.st_size, BLOCK_SIZE)) {
    std::cerr << "Invalid layout: file size not block-aligned for \""
              << pathname << "\" Fallback to syscall\n";
    is_ulayfs_file = false;
    return fd;
  }

  meta = mem_table.init(fd, stat_buf.st_size);
  allocator.init(fd, meta, &mem_table);

  tx_mgr = TxMgr(meta, &allocator, &mem_table);
  blk_table = BlkTable(meta, &mem_table, &tx_mgr);

  blk_table.update();

  if (stat_buf.st_size == 0) meta->init();
  return fd;
}
};  // namespace ulayfs::dram
