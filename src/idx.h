#pragma once

#include <bits/stdint-uintn.h>

#include <cstdint>

namespace ulayfs {

// block index within a file; the meta block has a LogicalBlockIdx of 0
using LogicalBlockIdx = uint32_t;
// block index seen by applications
using VirtualBlockIdx = uint32_t;

// local index within a block; this can be -1 to indicate an error
using BitmapLocalIdx = int32_t;
using TxLocalIdx = int32_t;
// Note: LogLocalIdx will persist and we need range [0, 256)
using LogLocalIdx = uint8_t;

// identifier of bitmap blocks; checkout BitmapBlock's doc to see more
using BitmapBlockId = uint32_t;

}  // namespace ulayfs
