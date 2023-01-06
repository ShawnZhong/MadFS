#include "file/file.h"

namespace ulayfs::dram {
off_t File::lseek(off_t offset, int whence) {
  int64_t ret;

  pthread_spin_lock(&spinlock);
  uint64_t file_size = blk_table.update();

  switch (whence) {
    case SEEK_SET:
      ret = offset_mgr.seek_absolute(offset);
      break;
    case SEEK_CUR:
      ret = offset_mgr.seek_relative(offset);
      if (ret == -1) errno = EINVAL;
      break;
    case SEEK_END:
      ret = offset_mgr.seek_absolute(static_cast<off_t>(file_size) + offset);
      break;
    case SEEK_DATA:
    case SEEK_HOLE:
    default:
      ret = -1;
      errno = EINVAL;
  }

  pthread_spin_unlock(&spinlock);
  return ret;
}
}  // namespace ulayfs::dram
