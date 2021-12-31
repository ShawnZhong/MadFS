#pragma once

#include "bitmap.h"
#include "entry.h"
#include "params.h"

namespace ulayfs {
// signature
constexpr static int SIGNATURE_SIZE = 8;
constexpr static char FILE_SIGNATURE[SIGNATURE_SIZE] = "ULAYFS";
constexpr static char SHM_XATTR_NAME[] = "user.shm_path";
constexpr static uint16_t SHM_PATH_LEN = 64;

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
    ((BLOCK_SIZE / CACHELINE_SIZE) - 2);
constexpr static uint16_t NUM_INLINE_TX_ENTRY =
    NUM_CL_TX_ENTRY_IN_META * NUM_TX_ENTRY_PER_CL;

// how many blocks a bitmap block can manage
constexpr static uint32_t BITMAP_BLOCK_CAPACITY_SHIFT =
    BITMAP_CAPACITY_SHIFT + (BLOCK_SHIFT - 3);  // 15
constexpr static uint32_t BITMAP_BLOCK_CAPACITY =
    1 << BITMAP_BLOCK_CAPACITY_SHIFT;

// total number of bitmaps in DRAM
// TODO: enable dynamic growing of bitmap
constexpr static uint16_t NUM_BITMAP_PER_BLOCK =
    BLOCK_SIZE / sizeof(dram::Bitmap);
static_assert(
    NUM_BITMAP_PER_BLOCK - 1 <= std::numeric_limits<BitmapIdx>::max(),
    "NUM_BITMAP_PER_BLOCK - 1 should be representable with BitmapIdx");

// we use one hugepage of bitmap, which is sufficient for a 64GB file
// TODO: extend bitmap dynamically
constexpr static uint32_t NUM_BITMAP_BLOCKS = 512;
constexpr static uint32_t BITMAP_SIZE = NUM_BITMAP_BLOCKS << BLOCK_SHIFT;
constexpr static uint32_t NUM_BITMAP = NUM_BITMAP_BLOCKS * NUM_BITMAP_PER_BLOCK;

// how many data blocks can be covered per CAS
// TODO: put this constant somewhere else?
constexpr static uint8_t MAX_BLOCKS_PER_BODY = 64;
constexpr static size_t MAX_BYTES_PER_BODY = MAX_BLOCKS_PER_BODY << BLOCK_SHIFT;
}  // namespace ulayfs
