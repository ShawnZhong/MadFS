#pragma once

#include "const.h"
#include "debug.h"
#include "utils.h"

namespace ulayfs::pmem {

class DataBlock : public noncopyable {
 public:
  char data[BLOCK_SIZE];
};

static_assert(sizeof(DataBlock) == BLOCK_SIZE,
              "DataBlock must be of size BLOCK_SIZE");

}  // namespace ulayfs::pmem
