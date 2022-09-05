#pragma once

#include <cstdint>
#include <limits>

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
 * block index
 */
constexpr static uint16_t VIRTUAL_BLOCK_IDX_SIZE = 4;
constexpr static uint16_t LOGICAL_BLOCK_IDX_SIZE = 4;

/*
 * tx entry
 */
constexpr static uint16_t TX_ENTRY_SIZE = 8;
constexpr static uint16_t NUM_TX_ENTRY_PER_BLOCK =
    (BLOCK_SIZE - 2 * LOGICAL_BLOCK_IDX_SIZE - 2 * sizeof(uint32_t)) /
    TX_ENTRY_SIZE;

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
constexpr static uint32_t BITMAP_BLOCK_CAPACITY_SHIFT = 6;
// how many blocks a bitmap can manage
constexpr static uint32_t BITMAP_BLOCK_CAPACITY =
    1 << BITMAP_BLOCK_CAPACITY_SHIFT;
// how many bytes a bitmap can manage
constexpr static uint64_t BITMAP_BYTES_CAPACITY = BITMAP_BLOCK_CAPACITY
                                                  << BLOCK_SHIFT;

// total number of bitmaps in DRAM
constexpr static uint16_t NUM_BITMAP_PER_BLOCK = BLOCK_SIZE / BITMAP_SIZE;

// we use one hugepage of bitmap, which is sufficient for a 64GB file
constexpr static uint32_t NUM_BITMAP_BLOCKS = 512;
constexpr static uint32_t SHM_SIZE = NUM_BITMAP_BLOCKS << BLOCK_SHIFT;
constexpr static uint32_t NUM_BITMAP = NUM_BITMAP_BLOCKS * NUM_BITMAP_PER_BLOCK;

constexpr static uint16_t NUM_CL_PER_BLOCK = BLOCK_SIZE / CACHELINE_SIZE;
constexpr static uint32_t NUM_OFFSET_QUEUE_SLOT = 16;

enum class Event : std::size_t {
  READ,
  WRITE,
  PREAD,
  PWRITE,
  OPEN,
  CLOSE,
  FSYNC,

  ALIGNED_TX,
  ALIGNED_TX_CTOR,
  ALIGNED_TX_EXEC,
  ALIGNED_TX_COPY,
  ALIGNED_TX_PREPARE,
  ALIGNED_TX_UPDATE,
  ALIGNED_TX_RECYCLE,
  ALIGNED_TX_WAIT_OFFSET,
  ALIGNED_TX_COMMIT,
  ALIGNED_TX_FREE,

  SINGLE_BLOCK_TX,
  SINGLE_BLOCK_TX_START,
  SINGLE_BLOCK_TX_COPY,
  SINGLE_BLOCK_TX_COMMIT,

  MULTI_BLOCK_TX,
  MULTI_BLOCK_TX_START,
  MULTI_BLOCK_TX_COPY,
  MULTI_BLOCK_TX_COMMIT,

  TX_ENTRY_LOAD,
  TX_ENTRY_STORE,
};
}  // namespace ulayfs
