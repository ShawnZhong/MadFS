#pragma once

#include "bitmap.h"
#include "entry.h"

namespace ulayfs {
// signature
constexpr static int SIGNATURE_SIZE = 16;
constexpr static char FILE_SIGNATURE[SIGNATURE_SIZE] = "ULAYFS";

constexpr static uint16_t NUM_CL_PER_BLOCK = BLOCK_SIZE / CACHELINE_SIZE;

// number of various data structures in blocks
constexpr static uint16_t NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(LogicalBlockIdx)) / sizeof(pmem::TxEntry);
constexpr static uint16_t NUM_LOG_ENTRY = BLOCK_SIZE / sizeof(pmem::LogEntry);

static_assert(NUM_TX_ENTRY - 1 <= std::numeric_limits<TxLocalIdx>::max(),
              "NUM_TX_ENTRY - 1 should be representable with TxLocalIdx");
static_assert(
    NUM_LOG_ENTRY - 1 <= std::numeric_limits<LogLocalUnpackIdx>::max(),
    "NUM_LOG_ENTRY - 1 should be representable with LogLocalUnpackIdx");

constexpr static uint16_t NUM_TX_ENTRY_PER_CL =
    CACHELINE_SIZE / sizeof(pmem::TxEntry);

// inline data structure count in meta block
constexpr static uint16_t NUM_CL_TX_ENTRY_IN_META =
    ((BLOCK_SIZE / CACHELINE_SIZE) - 3);
constexpr static uint16_t NUM_INLINE_TX_ENTRY =
    NUM_CL_TX_ENTRY_IN_META * NUM_TX_ENTRY_PER_CL;

// how many blocks a bitmap block can manage
constexpr static uint32_t BITMAP_BLOCK_CAPACITY_SHIFT =
    BITMAP_CAPACITY_SHIFT + (BLOCK_SHIFT - 3);  // 15
constexpr static uint32_t BITMAP_BLOCK_CAPACITY =
    1 << BITMAP_BLOCK_CAPACITY_SHIFT;

// total number of bitmaps in DRAM
// TODO: enable dynamic growing of bitmap
constexpr static uint16_t NUM_BITMAP = BLOCK_SIZE / sizeof(dram::Bitmap);
static_assert(NUM_BITMAP - 1 <= std::numeric_limits<BitmapLocalIdx>::max(),
              "NUM_BITMAP - 1 should be representable with BitmapLocalIdx");

constexpr static uint32_t NUM_BITMAP_BLKS = 8;
constexpr static uint32_t BITMAP_SIZE = NUM_BITMAP_BLKS * BLOCK_SIZE;
constexpr static uint32_t TOTAL_DRAM_BITMAP = NUM_BITMAP_BLKS * NUM_BITMAP;

constexpr static uint16_t NUM_BITMAP_PER_CL =
    CACHELINE_SIZE / sizeof(dram::Bitmap);

// how many data blocks can be covered per CAS
// TODO: put this constant somewhere else?
constexpr static uint8_t MAX_BLOCKS_PER_BODY = 64;
constexpr static size_t MAX_BYTES_PER_BODY = MAX_BLOCKS_PER_BODY * BLOCK_SIZE;
}  // namespace ulayfs
