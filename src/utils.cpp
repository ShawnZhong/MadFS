#include "utils.h"

#include <syscall.h>

// The following variables are declared as `extern` in the header file, and
// defined here, so that we don't need to have a private (i.e., static) variable
// for every translation unit.
thread_local const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
FILE *log_file = stderr;
