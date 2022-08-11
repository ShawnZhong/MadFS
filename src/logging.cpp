#include "logging.h"

#include <syscall.h>
#include <unistd.h>

namespace ulayfs {
// The following variables are declared as `extern` in the header file, and
// defined here, so that we don't need to have multiple private (i.e., static)
// variables for every translation unit.
thread_local const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
FILE *log_file = stderr;

}  // namespace ulayfs
