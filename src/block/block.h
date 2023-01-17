#pragma once

#include "block/log.h"
#include "block/meta.h"
#include "block/tx.h"

namespace madfs::pmem {

union Block {
  MetaBlock meta_block;
  TxBlock tx_block;
  LogEntryBlock log_entry_block;
  char data[BLOCK_SIZE];
  char cache_lines[NUM_CL_PER_BLOCK][CACHELINE_SIZE];

  [[nodiscard]] char* data_rw() { return data; }
  [[nodiscard]] const char* data_ro() const { return data; }

  /**
   * zero-out a cache line
   * @param cl_idx which cache line to zero out
   * @return whether memset happened
   */
  bool zero_init_cl(uint16_t cl_idx) {
    constexpr static const char zero_cl[CACHELINE_SIZE]{};
    if (memcmp(&cache_lines[cl_idx], zero_cl, CACHELINE_SIZE) == 0)
      return false;
    memset(&cache_lines[cl_idx], 0, CACHELINE_SIZE);
    persist_cl_unfenced(&cache_lines[cl_idx]);
    return true;
  }

  /**
   * zero-initialize a block within a given cache-line range
   * @param cl_idx_begin the beginning cache-line index
   * @param cl_idx_end the ending cache-line index
   * @return whether memset happened
   */
  bool zero_init(uint16_t cl_idx_begin = 0,
                 uint16_t cl_idx_end = NUM_CL_PER_BLOCK) {
    bool do_memset = false;
    for (uint16_t cl_idx = cl_idx_begin; cl_idx < cl_idx_end; ++cl_idx)
      do_memset |= zero_init_cl(cl_idx);
    if (do_memset) fence();
    return do_memset;
  }
};

static_assert(sizeof(Block) == BLOCK_SIZE, "Block must be of size BLOCK_SIZE");

}  // namespace madfs::pmem
