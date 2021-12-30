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
      TxEntryIdx tx_idx;
      const pmem::TxBlock* tx_block;
    } ticket_slot;
    char cl[CACHELINE_SIZE];

    TicketSlot() { ticket_slot.ticket.store(0, std::memory_order_relaxed); }
  };

  File* file;
  uint64_t offset;
  uint64_t next_ticket;
  TicketSlot queues[NUM_OFFSET_QUEUE_SLOT];

 public:
  explicit OffsetMgr(File* file)
      : file(file), offset(0), next_ticket(1), queues() {}

  // must have spinlock acquired
  // only call if seeking is the only serialization point
  // no boundary check
  int64_t seek_absolute(uint64_t abs_offset) {
    return static_cast<int64_t>(offset = abs_offset);
  }
  int64_t seek_relative(int64_t rel_offset) {
    return seek_absolute(offset + rel_offset);
  }

  /**
   * move the current offset and get the updated offset; not thread-safe so must
   * be called with spinlock held; must call release_offset after done
   *
   * @param[in,out] change movement applied to the offset, will be updated if
   * hit boundary and stop_at_boundary is set
   * @param[in] file_size the current file size for boundary check
   * @param[in] stop_at_boundary whether stop at the boundary
   * @param[out] ticket ticket for this acquire
   * @return old offset
   */
  uint64_t acquire_offset(uint64_t& change, uint64_t file_size,
                          bool stop_at_boundary, uint64_t& ticket);

  /**
   * wait for the previous one to complete; return previous one's idx and block
   *
   * @param[in] ticket ticket from acquire_offset
   * @return address of slot; null if no need to validate
   */
  const OffsetMgr::TicketSlot* wait_offset(uint64_t ticket);

  /**
   * validate whether redo is necessary; the previous operation's serialization
   * point should be no larger than the current one's
   *
   * @param[in] ticket ticket from acquire_offset
   * @param[in] curr_idx the tx tail idx seen by the current operation
   * @param[in] curr_block the tx tail block seen by the current operation
   * @return whether the ordering is fine (prev <= curr)
   */
  bool validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                       const pmem::TxBlock* curr_block);

  /**
   * release the offset
   *
   * @param[in] ticket ticket from acquire_offset
   * @param[in] curr_idx the tx tail idx seen by the current operation
   * @param[in] curr_block the tx tail block seen by the current operation
   */
  void release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                      const pmem::TxBlock* curr_block);

  friend std::ostream& operator<<(std::ostream& out, const OffsetMgr& o);
};

};  // namespace ulayfs::dram
