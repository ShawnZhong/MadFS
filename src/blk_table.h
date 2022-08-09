#pragma once

#include <pthread.h>
#include <tbb/concurrent_vector.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <type_traits>

#include "block/block.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "tx/mgr.h"
#include "utils.h"

namespace ulayfs::dram {

struct FileState {
  TxCursor cursor;
  uint64_t file_size;
};
static_assert(sizeof(FileState) == 24);

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  File* file;
  TxMgr* tx_mgr;

  tbb::concurrent_vector<LogicalBlockIdx, zero_allocator<LogicalBlockIdx>>
      table;

  FileState state;
  /**
   * Version of the file state above.
   * It can only be updated with file->spinlock held. Before a writer (of
   * BlkTable) tries to update the three fields above, it must increment the
   * version; after it's done, it must increment it again.
   * Thus, when this version is odd, it means someone is updating three fields
   * above and they are in a temporary inconsistent state.
   */
  std::atomic<uint64_t> version;

 public:
  explicit BlkTable(File* file, TxMgr* tx_mgr)
      : file(file), tx_mgr(tx_mgr), state(), version(0) {
    table.grow_to_at_least(NUM_BLOCKS_PER_GROW);
  }

  ~BlkTable() = default;

  /**
   * @return the logical block index corresponding the the virtual block index
   *  0 is returned if the virtual block index is not allocated yet
   */
  [[nodiscard]] LogicalBlockIdx get(VirtualBlockIdx virtual_block_idx) const {
    if (virtual_block_idx >= table.size()) return 0;
    return table[virtual_block_idx.get()];
  }

  /**
   * Update the block table by applying the transactions; not thread-safe
   *
   * @param do_alloc whether we allow allocation when iterating the tx_idx
   * @param init_bitmap whether we need to initialize the bitmap
   */
  uint64_t update(bool do_alloc, bool init_bitmap = false);

  /**
   * Quick check if update is necessary; thread safe
   * This check is guarantee to not write any shared data structure so avoid
   * cache coherence traffic. If this function return true, do not acquire
   * spinlock in file.
   *
   * @param[out] tx_idx if no need to update, return tx_idx
   * @param[out] tx_block if no need to update, return tx_block
   * @param[out] f_size if no need to update, return file_size
   * @param[in] do_alloc whether do allocation
   * @return whether update is necessary; if false, set tx_idx and tx_block;
   * otherwise leave them undefined.
   */
  [[nodiscard]] bool need_update(FileState* result_state, bool do_alloc) const {
    uint64_t curr_ver = version.load(std::memory_order_acquire);
    if (curr_ver & 1) return true;  // old version means inconsistency
    *result_state = state;
    if (curr_ver != version.load(std::memory_order_release)) return true;
    if (!tx_mgr->handle_cursor_overflow(&result_state->cursor, do_alloc))
      return false;
    // if it's not valid, there is no new tx to the tx history, thus no need to
    // acquire spinlock to update
    return tx_mgr->get_tx_entry(result_state->cursor).is_valid();
  }

  [[nodiscard]] TxEntryIdx get_tx_idx() const { return state.cursor.idx; }
  [[nodiscard]] FileState get_file_state() const { return state; }

 private:
  void grow_to_fit(VirtualBlockIdx idx);

  /**
   * Apply an indirect transaction to the block table
   *
   * @param tx_entry the entry to be applied
   * @param init_bitmap whether we need to initialize the bitmap object
   */
  void apply_indirect_tx(pmem::TxEntryIndirect tx_entry, bool init_bitmap);

  /**
   * Apply an inline transaction to the block table
   * @param tx_entry the entry to be applied
   */
  void apply_inline_tx(pmem::TxEntryInline tx_entry);

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b);
};

}  // namespace ulayfs::dram
