#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "bitmap.h"
#include "config.h"
#include "entry.h"
#include "futex.h"
#include "idx.h"
#include "params.h"
#include "utils.h"

namespace ulayfs {

// signature
constexpr static int SIGNATURE_SIZE = 16;
constexpr static char FILE_SIGNATURE[SIGNATURE_SIZE] = "ULAYFS";

// number of various data structures in blocks
constexpr static uint16_t NUM_BITMAP = BLOCK_SIZE / sizeof(pmem::Bitmap);
constexpr static uint16_t NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(LogicalBlockIdx)) / sizeof(pmem::TxEntry);
constexpr static uint16_t NUM_LOG_ENTRY = BLOCK_SIZE / sizeof(pmem::LogEntry);

static_assert(NUM_LOG_ENTRY <= 1 << 8,
              "NUM_LOG_ENTRY should be representable in less than 8 bits");

// inline data structure count in meta block
constexpr static uint32_t NUM_CL_BITMAP_IN_META = 32;
constexpr static uint32_t NUM_CL_TX_ENTRY_IN_META =
    ((BLOCK_SIZE / CACHELINE_SIZE) - 2) - NUM_CL_BITMAP_IN_META;
constexpr static uint32_t NUM_INLINE_BITMAP =
    NUM_CL_BITMAP_IN_META * (CACHELINE_SIZE / sizeof(pmem::Bitmap));
constexpr static uint32_t NUM_INLINE_TX_ENTRY =
    NUM_CL_TX_ENTRY_IN_META * (CACHELINE_SIZE / sizeof(pmem::TxEntry));

// how many blocks a bitmap block can manage
constexpr static uint32_t BITMAP_BLOCK_CAPACITY_SHIFT =
    BITMAP_CAPACITY_SHIFT + (BLOCK_SHIFT - 3);  // 15
constexpr static uint32_t BITMAP_BLOCK_CAPACITY =
    1 << BITMAP_BLOCK_CAPACITY_SHIFT;

// 32 cacheline corresponds to 2^14 bits
constexpr static uint32_t INLINE_BITMAP_CAPACITY =
    NUM_INLINE_BITMAP * BITMAP_CAPACITY;

};  // namespace ulayfs
