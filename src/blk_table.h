#pragma once

#include <tbb/concurrent_vector.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <type_traits>

#include "bitmap.h"
#include "block/block.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "tx/cursor.h"
#include "utils.h"

namespace ulayfs::dram {
struct FileState {
  TxCursor cursor;
  uint64_t file_size;
};
static_assert(sizeof(FileState) == 24);

class File;
class TxMgr;

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  File* file;
  TxMgr* tx_mgr;

  tbb::concurrent_vector<std::atomic<LogicalBlockIdx>,
                         zero_allocator<std::atomic<LogicalBlockIdx>>>
      table;
  static_assert(std::atomic<LogicalBlockIdx>::is_always_lock_free);

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
  explicit BlkTable(File* file, pmem::MetaBlock* meta, TxMgr* tx_mgr)
      : file(file), tx_mgr(tx_mgr), state({meta, 0}), version(0) {
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
   * @param bitmap_mgr if passed, initialized the bitmap
   */
  uint64_t update(bool do_alloc, BitmapMgr* bitmap_mgr = nullptr);

  /**
   * Quick check if update is necessary; thread safe
   * This check is guarantee to not write any shared data structure so avoid
   * cache coherence traffic. If this function return true, do not acquire
   * spinlock in file.
   *
   * @param result_state if no need to update, file state is stored here
   * @param do_alloc whether do allocation
   * @return whether update is necessary
   */
  [[nodiscard]] bool need_update(FileState* result_state, bool do_alloc) const;

  [[nodiscard]] TxEntryIdx get_tx_idx() const { return state.cursor.idx; }
  [[nodiscard]] FileState get_file_state() const { return state; }

 private:
  void grow_to_fit(VirtualBlockIdx idx);

  /**
   * Apply an indirect transaction to the block table
   *
   * @param tx_entry the entry to be applied
   * @param bitmap_mgr if passed, initialized the bitmap
   */
  void apply_indirect_tx(pmem::TxEntryIndirect tx_entry, BitmapMgr* bitmap_mgr);

  /**
   * Apply an inline transaction to the block table
   * @param tx_entry the entry to be applied
   */
  void apply_inline_tx(pmem::TxEntryInline tx_entry);

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b);
};

}  // namespace ulayfs::dram
