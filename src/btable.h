#pragma once

#include <unordered_map>

#include "layout.h"
namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlockTable {
  std::unordered_map<pmem::VirtualBlockIdx, pmem::LogicalBlockIdx> table;
};

};  // namespace ulayfs::dram
