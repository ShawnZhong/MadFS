#pragma once

#include "file.h"

namespace ulayfs {
extern std::shared_ptr<dram::File> get_file(int fd);
}  // namespace ulayfs
