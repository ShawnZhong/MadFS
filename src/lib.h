#pragma once

#include "file.h"
#include <memory>

namespace ulayfs {
std::shared_ptr<dram::File> get_file(int fd);
}  // namespace ulayfs
