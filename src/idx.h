#pragma once

#include <cstdint>

namespace ulayfs {

// block index within a file; the meta block has a LogicalBlockIdx of 0
using LogicalBlockIdx = uint32_t;
// block index seen by applications
using VirtualBlockIdx = uint32_t;

// local index within a block; this can be -1 to indicate an error
using BitmapLocalIdx = int16_t;
using TxLocalIdx = int16_t;
// Note: LogLocalIdx will persist and the valid range is [0, 255]
using LogLocalIdx = uint16_t;

// identifier of bitmap blocks; checkout BitmapBlock's doc to see more
using BitmapBlockId = uint32_t;

}  // namespace ulayfs
