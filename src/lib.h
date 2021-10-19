#pragma once

#include <unordered_map>

#include "file.h"

namespace ulayfs {
// mapping between fd and in-memory file handle
std::unordered_map<int, dram::File *> files;
}  // namespace ulayfs
