#include "lib.h"
#include "timer.h"

namespace ulayfs {
extern "C" {
int close(int fd) {
  if (auto file = get_file(fd)) {
    LOG_DEBUG("ulayfs::close(%s)", file->path);
    files.unsafe_erase(fd);
    timer.count<Event::CLOSE>();
    return 0;
  } else {
    LOG_DEBUG("posix::close(%d)", fd);
    return posix::close(fd);
  }
}

int fclose(FILE* stream) {
  static DEFINE_FN(fclose);

  int fd = fileno(stream);
  if (auto file = get_file(fd)) {
    LOG_DEBUG("ulayfs::fclose(%s)", file->path);
    files.unsafe_erase(fd);
    return 0;
  } else {
    LOG_DEBUG("posix::fclose(%p)", stream);
    return fclose(stream);
  }
}
}
}  // namespace ulayfs
