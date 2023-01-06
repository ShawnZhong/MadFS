#include "lib.h"
#include "utils/timer.h"

namespace ulayfs {
extern "C" {
void* mmap(void* addr, size_t length, int prot, int flags, int fd,
           off_t offset) {
  if (auto file = get_file(fd)) {
    void* ret =
        file->mmap(addr, length, prot, flags, static_cast<size_t>(offset));
    LOG_DEBUG("ulayfs::mmap(%p, %zu, %x, %x, %d, %ld) = %p", addr, length, prot,
              flags, fd, offset, ret);
    return ret;
  } else {
    void* ret = posix::mmap(addr, length, prot, flags, fd, offset);
    LOG_DEBUG("posix::mmap(%p, %zu, %x, %x, %d, %ld) = %p", addr, length, prot,
              flags, fd, offset, ret);
    return ret;
  }
}

void* mmap64(void* addr, size_t length, int prot, int flags, int fd,
             off64_t offset) {
  return mmap(addr, length, prot, flags, fd, offset);
}
}
}  // namespace ulayfs
