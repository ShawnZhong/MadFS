#pragma once

#include "file.h"

namespace ulayfs {
std::shared_ptr<dram::File> get_file(int fd);

/**
 * Open the shared memory object corresponding to this file and save the mmapped
 * address to bitmap. The leading bit of the bitmap (corresponding to metablock)
 * indicates if the bitmap needs to be initialized.
 *
 * @param[in] pathname path to the file
 * @param[in] stat stat of the original file
 * @param[out] bitmap the bitmap opened or created in the shared memory
 * @return the file descriptor for the shared memory object on success,
 * -1 otherwise
 */
int open_shm(const char* pathname, const struct stat* stat,
             pmem::Bitmap*& bitmap);
}  // namespace ulayfs
