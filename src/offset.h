#pragma once

#include <atomic>
#include <cstdint>
#include <iosfwd>

#include "block/block.h"
#include "const.h"
#include "cursor/tx_entry.h"
#include "idx.h"

namespace ulayfs::dram {

class OffsetMgr {
  union TicketSlot {
    struct {
      std::atomic<uint64_t> ticket;
      TxCursor cursor;
    } ticket_slot;
    char cl[CACHELINE_SIZE];

    TicketSlot() { ticket_slot.ticket.store(0, std::memory_order_relaxed); }
  };

  off_t offset;
  uint64_t next_ticket;
  TicketSlot queues[NUM_OFFSET_QUEUE_SLOT];

 public:
  explicit OffsetMgr() : offset(0), next_ticket(1), queues() {}

  // must have spinlock acquired
  // only call if seeking is the only serialization point
  // no boundary check
  off_t seek_absolute(off_t new_offset) { return offset = new_offset; }
  off_t seek_relative(off_t diff) {
    if ((diff > 0 && offset > std::numeric_limits<off_t>::max() - diff) ||
        (diff < 0 && offset < std::numeric_limits<off_t>::min() - diff)) {
      return -1;
    }
    off_t new_offset = offset + diff;
    return seek_absolute(new_offset);
  }

  /**
   * move the current offset and get the updated offset; not thread-safe so must
   * be called with spinlock held; must call release after done
   *
   * @param[in,out] count movement applied to the offset, will be updated if
   * hit boundary and stop_at_boundary is set
   * @param[in] file_size the current file size for boundary check
   * @param[in] stop_at_boundary whether stop at the boundary
   * @param[out] ticket ticket for this acquire
   * @return old offset
   */
  uint64_t acquire(uint64_t& count, uint64_t file_size, bool stop_at_boundary,
                   uint64_t& ticket) {
    off_t old_offset = offset;
    offset += static_cast<off_t>(count);
    if (stop_at_boundary && static_cast<uint64_t>(offset) > file_size) {
      offset = static_cast<off_t>(file_size);
      count = file_size - static_cast<uint64_t>(old_offset);
    }
    ticket = next_ticket++;
    return static_cast<uint64_t>(old_offset);
  }

  /**
   * wait for the previous one to complete; return previous one's idx and block
   *
   * @param[in] ticket ticket from acquire
   * @return address of slot; null if no need to validate
   */
  const OffsetMgr::TicketSlot* wait(uint64_t ticket) {
    // if we don't want strict serialization on offset, always return
    // immediately
    if (!runtime_options.strict_offset_serial) return nullptr;
    uint64_t prev_ticket = ticket - 1;
    if (prev_ticket == 0) return nullptr;
    const TicketSlot* slot = &queues[prev_ticket % NUM_OFFSET_QUEUE_SLOT];
    while (slot->ticket_slot.ticket.load(std::memory_order_acquire) !=
           prev_ticket)
      _mm_pause();
    return slot;
  }

  /**
   * validate whether redo is necessary; the previous operation's serialization
   * point should be no larger than the current one's
   *
   * @param ticket ticket from acquire
   * @param cursor the cursor seen by the current operation
   * @return whether the ordering is fine (prev <= curr)
   */
  bool validate(uint64_t ticket, TxCursor cursor) {
    // if we don't want strict serialization on offset, always return
    // immediately
    if (!runtime_options.strict_offset_serial) return true;
    const TicketSlot* slot = wait(ticket);
    // no previous operation to validate against
    if (!slot) return true;
    if (slot->ticket_slot.cursor < cursor) return true;
    return false;
  }

  /**
   * release the offset
   *
   * @param ticket ticket from acquire
   * @param cursor the cursor seen by the current operation
   */
  void release(uint64_t ticket, TxCursor cursor) {
    // if we don't want strict serialization on offset, always return
    // immediately
    if (!runtime_options.strict_offset_serial) return;
    TicketSlot* slot = &queues[ticket % NUM_OFFSET_QUEUE_SLOT];
    slot->ticket_slot.cursor = cursor;
    slot->ticket_slot.ticket.store(ticket, std::memory_order_release);
  }

  friend std::ostream& operator<<(std::ostream& out, const OffsetMgr& o) {
    out << "OffsetMgr: offset = " << o.offset << "\n";
    return out;
  }
};

};  // namespace ulayfs::dram
