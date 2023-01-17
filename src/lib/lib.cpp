#include "lib.h"

#include <cstdio>
#include <iostream>

#include "config.h"

namespace madfs {
extern "C" {
/**
 * Called when the shared library is first loaded
 *
 * Note that the global variables may not be initialized at this point
 * e.g., all the functions in the madfs::posix namespace
 */
void __attribute__((constructor)) madfs_ctor() {
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
void __attribute__((destructor)) madfs_dtor() {
  std::cerr << "MadFS unloaded" << std::endl;
}
}  // extern "C"
}  // namespace madfs
