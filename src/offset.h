#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

#include "block.h"
#include "config.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "params.h"

namespace ulayfs::dram {

class File;

class OffsetMgr {
  union TicketSlot {
    struct {
      std::atomic_uint64_t ticket;
      TxEntryIdx tail_tx_idx;
      const pmem::TxBlock* tail_tx_block;
    } ticket_slot;
    char cl[CACHELINE_SIZE];

    TicketSlot() { ticket_slot.ticket.store(0, std::memory_order_relaxed); }
  };

  File* file;
  int64_t offset;
  uint64_t next_ticket;
  TicketSlot queues[NUM_OFFSET_QUEUE_SLOT];

 public:
  OffsetMgr(File* file) : file(file), offset(0), next_ticket(1), queues() {}

  // must have spinlock acquired
  // only call if seeking is the only serialization point
  // always with boundary check
  int64_t seek_absolute(int64_t abs_offset, int file_size) {
    if (abs_offset < 0 || abs_offset >= file_size) return -1;
    return offset = abs_offset;
  }
  int64_t seek_relative(int64_t rel_offset, int file_size) {
    return seek_absolute(offset + rel_offset, file_size);
  }

  /**
   * move the current offset and get the updated offset; not thread-safe so must
   * be called with spinlock held; must call release_offset after done
   *
   * @param[in] change movement applied to the offset (can be negative)
   * @param[in] file_size the current file size for boundary check
   * @param[in] stop_at_boundary whether stop at the boundary
   * @param[out] ticket ticket for this acquire
   * @return new offset
   */
  int64_t acquire_offset(int64_t change, int64_t file_size,
                         bool stop_at_boundary, uint64_t& ticket);

  /**
   * wait for the previous one to complete; return previous one's idx and block
   *
   * @param[in] ticket ticket from acquire_offset
   * @param[out] prev_idx the previous operation's tx idx (serialization point)
   * @param[out] prev_block the previous operation's tx block
   * @return whether need to validate againest prev (if false, prev_idx and
   * prev_block are undefined)
   */
  bool wait_offset(uint64_t ticket, TxEntryIdx& prev_idx,
                   const pmem::TxBlock*& prev_block);

  /**
   * validate whether redo is necessary; the previous operation's serialization
   * point should be no larger than the current one's
   *
   * @param[in] ticket ticket from acquire_offset
   * @param[in] curr_idx the tx tail idx seen by the current operation
   * @param[in] curr_block the tx tail block seen by the current operation
   * @param[out] prev_idx the tx tail idx seen by the previous operation
   * @param[out] prev_block the tx block idx seen by the previous operation
   * @return whether the ordering is fine (prev <= curr)
   */
  bool validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                       const pmem::TxBlock* curr_block, TxEntryIdx& prev_idx,
                       const pmem::TxBlock*& prev_block);

  /**
   * release the offset
   *
   * @param[in] ticket ticket from acquire_offset
   * @param[out] curr_idx the tx tail idx seen by the current operation
   * @param[out] curr_block the tx tail block seen by the current operation
   */
  void release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                      const pmem::TxBlock* curr_block);

  friend std::ostream& operator<<(std::ostream& out, const OffsetMgr& o);
};

};  // namespace ulayfs::dram
