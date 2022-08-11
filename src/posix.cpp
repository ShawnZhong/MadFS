#include "posix.h"

namespace ulayfs::posix {

DEFINE_FN(lseek);
DEFINE_FN(write);
DEFINE_FN(pwrite);
DEFINE_FN(read);
DEFINE_FN(pread);
DEFINE_FN(open);
DEFINE_FN(close);
DEFINE_FN(mmap);
DEFINE_FN(mremap);
DEFINE_FN(munmap);
DEFINE_FN(fallocate);
DEFINE_FN(ftruncate);
DEFINE_FN(fsync);
DEFINE_FN(fdatasync);
DEFINE_FN(flock);
DEFINE_FN(fcntl);
DEFINE_FN(unlink);
DEFINE_FN(rename);
DEFINE_FN(__fxstat);

}  // namespace ulayfs::posix
