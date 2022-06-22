#pragma once

#include <memory>

#include "file.h"

namespace ulayfs {
std::shared_ptr<dram::File> get_file(int fd);
}  // namespace ulayfs
