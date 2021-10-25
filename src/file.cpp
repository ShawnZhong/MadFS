#include "file.h"

namespace ulayfs::dram {

int File::open(const char* pathname, int flags, mode_t mode) {
  int ret;
  fd = posix::open(pathname, flags, mode);
  if (fd < 0) return fd;  // fail to open the file
  open_flags = flags;

  struct stat stat_buf;
  ret = posix::fstat(fd, &stat_buf);
  panic_if(ret, "fstat failed");

  meta = mtable.init(fd, stat_buf.st_size);
  allocator.init(fd, meta, &mtable);
  tx_mgr.init(meta, &allocator, &mtable, &btable);

  if (stat_buf.st_size == 0) meta->init();
  return fd;
}

std::ostream& operator<<(std::ostream& out, const File& f) {
  out << "fd: " << f.fd << "\n";
  out << *f.meta;
  out << f.mtable;
  return out;
}

};  // namespace ulayfs::dram
