#pragma once

#include <cstdint>

#include "config.h"

namespace ulayfs::pmem {

using BlockIdx = uint32_t;
using Bitmap = uint64_t;

class LogEntry;

class TxEntry {
  uint64_t tx_entry;
};

class TxBegin : TxEntry {};

class TxCommit : TxEntry {};

class LogEntry {
  uint32_t op;
  BlockIdx file_offset;
  BlockIdx block_offset;
  uint32_t size;
};

constexpr static int NUM_BITMAP = BLOCK_SIZE / sizeof(Bitmap);
constexpr static int NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(BlockIdx)) / sizeof(TxEntry);
constexpr static int NUM_LOG_ENTRY = BLOCK_SIZE / sizeof(LogEntry);
constexpr static int NUM_INLINE_BITMAP = 24;
constexpr static int NUM_INLINE_TX_ENTRY = 480;

/*
 * Idx: 0          1          2          3
 * +----------+----------+----------+----------+----------+----------+----------
 * |   Meta   | Bitmap 1 | Bitmap 2 |   ...    |   ...    | Data/Log |   ...
 * +----------+----------+----------+----------+----------+----------+----------
 * Note: The first few blocks following the meta block is always bitmap blocks
 */

class MetaBlock {
  // file size in bytes
  uint64_t file_size;

  // address for futex to lock
  uint32_t meta_lock;

  // number of blocks following the meta block that are bitmap blocks
  uint32_t num_bitmap_blocks;

  // if inline_tx_entries is used up, this points to the next log block
  BlockIdx log_head;

  // hint to find log tail; not necessarily up-to-date
  BlockIdx log_tail;

  // padding avoid cache line contention
  char padding[40];

  // for the rest of 63 cache lines:
  // 3 cache lines for bitmaps (~1536 blocks)
  Bitmap inline_bitmaps[NUM_INLINE_BITMAP];

  // 60 cache lines for tx log (~480 txs)
  TxEntry inline_tx_entries[NUM_INLINE_TX_ENTRY];
};

class BitmapBlock {
  Bitmap bitmaps[NUM_BITMAP];
};

class TxLogBlock {
  BlockIdx prev;
  BlockIdx next;
  TxEntry tx_entries[NUM_TX_ENTRY];
};

class LogEntryBlock {
  LogEntry log_entries[NUM_LOG_ENTRY];
};

class DataBlock {
  char data[BLOCK_SIZE];
};

union Block {
  MetaBlock meta_block;
  BitmapBlock bitmap_block;
  TxLogBlock tx_log_block;
  LogEntryBlock log_entry_block;
  DataBlock data_block;
  char padding[BLOCK_SIZE];
};

static_assert(sizeof(Bitmap) == 8, "Bitmap must of 64 bits");
static_assert(sizeof(TxEntry) == 8, "TxEntry must be 64 bits");
static_assert(sizeof(LogEntry) == 16, "LogEntry must of size 16 bytes");
static_assert(sizeof(Block) == BLOCK_SIZE, "Block must be of size BLOCK_SIZE");

};  // namespace ulayfs::pmem
