#pragma once

#include <cstdint>
#include <limits>

namespace madfs {
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
constexpr static char FILE_SIGNATURE[SIGNATURE_SIZE] = "MADFS";
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
constexpr static uint16_t BITMAP_ENTRY_SIZE = 8;
constexpr static uint32_t BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT = 6;
// how many blocks a bitmap can manage
constexpr static uint32_t BITMAP_ENTRY_BLOCKS_CAPACITY =
    1 << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
// how many bytes a bitmap can manage
constexpr static uint64_t BITMAP_ENTRY_BYTES_CAPACITY =
    BITMAP_ENTRY_BLOCKS_CAPACITY << BLOCK_SHIFT;

constexpr static uint16_t NUM_BITMAP_ENTRIES_PER_BLOCK =
    BLOCK_SIZE / BITMAP_ENTRY_SIZE;

// we use 511 blocks for bitmap, which is sufficient for ~64GB file
constexpr static uint32_t NUM_BITMAP_BLOCKS = 511;
constexpr static uint32_t NUM_BITMAP_ENTRIES =
    NUM_BITMAP_BLOCKS * NUM_BITMAP_ENTRIES_PER_BLOCK;
constexpr static uint32_t TOTAL_NUM_BITMAP_BYTES =
    NUM_BITMAP_BLOCKS * BLOCK_SIZE;

constexpr static uint16_t NUM_CL_PER_BLOCK = BLOCK_SIZE / CACHELINE_SIZE;
constexpr static uint32_t NUM_OFFSET_QUEUE_SLOT = 16;

// the last one is used for garbage collection
constexpr static uint32_t SHM_GC_SIZE = BLOCK_SIZE;
constexpr static uint32_t SHM_PER_THREAD_SIZE = CACHELINE_SIZE;
constexpr static uint32_t MAX_NUM_THREADS = SHM_GC_SIZE / SHM_PER_THREAD_SIZE;
constexpr static uint32_t SHM_SIZE =
    NUM_BITMAP_BLOCKS * BLOCK_SIZE + SHM_GC_SIZE;
}  // namespace madfs
