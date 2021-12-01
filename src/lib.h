#pragma once

#include <sys/stat.h>

#include "bitmap.h"
#include "params.h"

namespace ulayfs {

extern void print_file(int fd) __attribute__((weak));

/**
 * Open the shared memory object corresponding to this file. The leading bit
 * of the bitmap (corresponding to metablock) indicates if the bitmap needs to
 * be initialized.
 *
 * @return Return a pointer to the mmapped bitmap object, or nullptr on
 * failure.
 */
extern pmem::Bitmap* open_shm(const char* pathname, const struct stat* stat);

}  // namespace ulayfs
