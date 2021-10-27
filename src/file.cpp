#include "file.h"

namespace ulayfs::dram {

int File::open(const char* pathname, int flags, mode_t mode) {
  int ret;
  fd = posix::open(pathname, flags, mode);
  if (fd < 0) return fd;  // fail to open the file
  open_flags = flags;

  struct stat stat_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  ret = posix::fstat(fd, &stat_buf);
  panic_if(ret, "fstat failed");

  meta = mtable.init(fd, stat_buf.st_size);
  allocator.init(fd, meta, &mtable);

  tx_mgr = TxMgr(meta, &allocator, &mtable);
  btable = BlkTable(meta, &mtable, &tx_mgr);

  btable.update();

  if (stat_buf.st_size == 0) meta->init();
  return fd;
}
};  // namespace ulayfs::dram
