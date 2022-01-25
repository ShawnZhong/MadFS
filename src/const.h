#pragma once

#include <cstdint>
#include <limits>

#include "idx.h"

namespace ulayfs {
/*
 * hardware
 */
constexpr static uint32_t BLOCK_SHIFT = 12;
constexpr static uint64_t BLOCK_SIZE = 1 << BLOCK_SHIFT;
constexpr static uint32_t CACHELINE_SHIFT = 6;
constexpr static uint32_t CACHELINE_SIZE = 1 << CACHELINE_SHIFT;

/*
 * file
 */
constexpr static int SIGNATURE_SIZE = 8;
constexpr static char FILE_SIGNATURE[SIGNATURE_SIZE] = "ULAYFS";
constexpr static char SHM_XATTR_NAME[] = "user.shm_path";
constexpr static uint16_t SHM_PATH_LEN = 64;
// grow in the unit of 2 MB
constexpr static uint32_t GROW_UNIT_SHIFT = 21;
constexpr static uint32_t GROW_UNIT_SIZE = 1 << GROW_UNIT_SHIFT;
// preallocate must be multiple of grow_unit
constexpr static uint32_t PREALLOC_SHIFT = 1 * GROW_UNIT_SHIFT;
constexpr static uint32_t PREALLOC_SIZE = 1 * GROW_UNIT_SIZE;
constexpr static uint32_t NUM_BLOCKS_PER_GROW = GROW_UNIT_SIZE >> BLOCK_SHIFT;

/*
 * tx entry
 */
constexpr static uint16_t TX_ENTRY_SIZE = 8;
constexpr static uint16_t NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(LogicalBlockIdx)) / TX_ENTRY_SIZE;
static_assert(NUM_TX_ENTRY - 1 <= std::numeric_limits<TxLocalIdx>::max(),
              "NUM_TX_ENTRY - 1 should be representable with TxLocalIdx");

// inline data structure count in meta block
constexpr static uint16_t NUM_TX_ENTRY_PER_CL = CACHELINE_SIZE / TX_ENTRY_SIZE;
constexpr static uint16_t NUM_CL_TX_ENTRY_IN_META =
    ((BLOCK_SIZE / CACHELINE_SIZE) - 2);
constexpr static uint16_t NUM_INLINE_TX_ENTRY =
    NUM_CL_TX_ENTRY_IN_META * NUM_TX_ENTRY_PER_CL;

/*
 * bitmap
 */
constexpr static uint16_t BITMAP_SIZE = 8;
// how many blocks a bitmap can manage
// (that's why call it "capacity" instead of "size")
constexpr static uint32_t BITMAP_CAPACITY_SHIFT = 6;
constexpr static uint32_t BITMAP_CAPACITY = 1 << BITMAP_CAPACITY_SHIFT;
// how many bytes are covered by a single 64-bit bitmap
constexpr static uint64_t BITMAP_CAPACITY_IN_BYTES = BITMAP_CAPACITY
                                                     << BLOCK_SHIFT;

// total number of bitmaps in DRAM
// TODO: enable dynamic growing of bitmap
constexpr static uint16_t NUM_BITMAP_PER_BLOCK = BLOCK_SIZE / BITMAP_SIZE;
static_assert(
    NUM_BITMAP_PER_BLOCK - 1 <= std::numeric_limits<BitmapIdx>::max(),
    "NUM_BITMAP_PER_BLOCK - 1 should be representable with BitmapIdx");

// we use one hugepage of bitmap, which is sufficient for a 64GB file
// TODO: extend bitmap dynamically
constexpr static uint32_t NUM_BITMAP_BLOCKS = 512;
constexpr static uint32_t SHM_SIZE = NUM_BITMAP_BLOCKS << BLOCK_SHIFT;
constexpr static uint32_t NUM_BITMAP = NUM_BITMAP_BLOCKS * NUM_BITMAP_PER_BLOCK;

constexpr static uint16_t NUM_CL_PER_BLOCK = BLOCK_SIZE / CACHELINE_SIZE;
constexpr static uint32_t NUM_OFFSET_QUEUE_SLOT = 16;
}  // namespace ulayfs
