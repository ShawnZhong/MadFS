#include "file/file.h"

namespace madfs::dram {
off_t File::lseek(off_t offset, int whence) {
  int64_t ret;

  blk_table.update([&](const FileState& state) {
    uint64_t file_size = state.file_size;
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
  });

  return ret;
}
}  // namespace madfs::dram
