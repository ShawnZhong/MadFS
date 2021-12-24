#include "offset.h"

#include <atomic>
#include <cstdint>
#include <cstring>

#include "block.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "params.h"

namespace ulayfs::dram {

int64_t OffsetMgr::acquire_offset(int64_t change, int64_t file_size,
                                  bool stop_at_boundary, uint64_t& ticket) {
  int64_t new_offset = offset + change;
  if (stop_at_boundary) {
    if (new_offset < 0)
      new_offset = 0;
    else if (new_offset > file_size)
      new_offset = file_size - 1;
  }
  ticket = next_ticket++;
  return offset = new_offset;
}

bool OffsetMgr::wait_offset(uint64_t ticket, TxEntryIdx& prev_idx,
                            const pmem::TxBlock*& prev_block) {
  uint64_t prev_ticket = ticket - 1;
  if (prev_ticket == 0) return false;
  TicketSlot& slot = queues[prev_ticket % NUM_OFFSET_QUEUE_SLOT];
  while (slot.ticket_slot.ticket.load(std::memory_order_acquire) != prev_ticket)
    asm volatile("pause");
  return true;
}

bool OffsetMgr::validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                                const pmem::TxBlock* curr_block,
                                TxEntryIdx& prev_idx,
                                const pmem::TxBlock*& prev_block) {
  bool need_validate = wait_offset(ticket, prev_idx, prev_block);
  if (!need_validate) return true;
  if (!file->tx_idx_greater(prev_idx, curr_idx, prev_block, curr_block))
    return true;
  return false;
}

void OffsetMgr::release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                               const pmem::TxBlock* curr_block) {
  TicketSlot& slot = queues[ticket % NUM_OFFSET_QUEUE_SLOT];
  slot.ticket_slot.tail_tx_idx = curr_idx;
  slot.ticket_slot.tail_tx_block = curr_block;
  slot.ticket_slot.ticket.store(ticket, std::memory_order_release);
}

std::ostream& operator<<(std::ostream& out, const OffsetMgr& o) {
  out << "OffsetMgr: offset = " << o.offset << "\n";
  return out;
}

};  // namespace ulayfs::dram
