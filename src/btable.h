#pragma once

#include <unordered_map>

#include "layout.h"
namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  //  std::unordered_map<pmem::VirtualBlockIdx, pmem::LogicalBlockIdx> table;

  std::vector<pmem::LogicalBlockIdx> table;

 public:
  BlkTable() = default;

  void put(pmem::VirtualBlockIdx key, pmem::LogicalBlockIdx val) {
    table[key] = val;
  }

  pmem::LogicalBlockIdx get(pmem::VirtualBlockIdx key) { return table[key]; }
};

};  // namespace ulayfs::dram
