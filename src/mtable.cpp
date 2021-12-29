#include "mtable.h"

namespace ulayfs::dram {
std::atomic<pmem::Block*> MemTable::next_hint = nullptr;
}  // namespace ulayfs::dram
