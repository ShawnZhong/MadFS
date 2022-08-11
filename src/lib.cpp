#include "lib.h"

#include <syscall.h>
#include <unistd.h>

#include <cstdio>
#include <iostream>

#include "config.h"
#include "file.h"
#include "timer.h"

namespace ulayfs {

thread_local Timer timer;
thread_local const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
FILE* log_file = stderr;

bool initialized = false;

// mapping between fd and in-memory file handle
// shared across threads within the same process
tbb::concurrent_unordered_map<int, std::shared_ptr<dram::File>> files;

extern "C" {
/**
 * Called when the shared library is first loaded
 *
 * Note that the global variables may not be initialized at this point
 * e.g., all the functions in the ulayfs::posix namespace
 */
void __attribute__((constructor)) ulayfs_ctor() {
  initialized = true;
  std::cerr << build_options << std::endl;
  std::cerr << runtime_options << std::endl;
  if (runtime_options.log_file) {
    log_file = fopen(runtime_options.log_file, "a");
  }
}

/**
 * Called when the shared library is unloaded
 */
void __attribute__((destructor)) ulayfs_dtor() {
  LOG_INFO("ulayfs_dtor called");
}
}  // extern "C"
}  // namespace ulayfs
