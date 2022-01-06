#include "offset.h"

#include <atomic>
#include <cstdint>
#include <cstring>

#include "block.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

uint64_t OffsetMgr::acquire_offset(uint64_t& change, uint64_t file_size,
                                   bool stop_at_boundary, uint64_t& ticket) {
  auto old_offset = offset;
  offset += change;
  if (stop_at_boundary && offset > file_size) {
    offset = file_size;
    change = offset - old_offset;
  }
  ticket = next_ticket++;
  return old_offset;
}

const OffsetMgr::TicketSlot* OffsetMgr::wait_offset(uint64_t ticket) {
  // if we don't want strict serialization on offset, always return immediately
  if (!runtime_options.strict_offset_serial) return nullptr;
  uint64_t prev_ticket = ticket - 1;
  if (prev_ticket == 0) return nullptr;
  const TicketSlot* slot = &queues[prev_ticket % NUM_OFFSET_QUEUE_SLOT];
  while (slot->ticket_slot.ticket.load(std::memory_order_acquire) !=
         prev_ticket)
    _mm_pause();
  return slot;
}

bool OffsetMgr::validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                                const pmem::TxBlock* curr_block) {
  // if we don't want strict serialization on offset, always return immediately
  if (!runtime_options.strict_offset_serial) return true;
  const TicketSlot* slot = wait_offset(ticket);
  // no previous operation to validate against
  if (!slot) return true;
  if (!file->tx_idx_greater(slot->ticket_slot.tx_idx, curr_idx,
                            slot->ticket_slot.tx_block, curr_block))
    return true;
  return false;
}

void OffsetMgr::release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                               const pmem::TxBlock* curr_block) {
  // if we don't want strict serialization on offset, always return immediately
  if (!runtime_options.strict_offset_serial) return;
  TicketSlot* slot = &queues[ticket % NUM_OFFSET_QUEUE_SLOT];
  slot->ticket_slot.tx_idx = curr_idx;
  slot->ticket_slot.tx_block = curr_block;
  slot->ticket_slot.ticket.store(ticket, std::memory_order_release);
}

std::ostream& operator<<(std::ostream& out, const OffsetMgr& o) {
  out << "OffsetMgr: offset = " << o.offset << "\n";
  return out;
}

};  // namespace ulayfs::dram
