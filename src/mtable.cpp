#include "mtable.h"

std::atomic<char*> ulayfs::dram::MemTable::next_hint = nullptr;
