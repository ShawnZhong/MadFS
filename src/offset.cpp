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
                                  bool check_boundary, bool error_if_out,
                                  uint64_t& ticket) {
  int64_t new_offset = offset + change;
  if (check_boundary) {
    if (new_offset < 0) {
      if (error_if_out) return -1;
      new_offset = 0;
    } else if (new_offset > file_size) {
      if (error_if_out) return -1;
      new_offset = file_size - 1;
    }
  }
  ticket = next_ticket++;
  return offset = new_offset;
}

bool OffsetMgr::validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                                const pmem::TxBlock* curr_block,
                                TxEntryIdx& prev_idx,
                                const pmem::TxBlock*& prev_block) {
  // nothing to validate
  if (ticket == 0) return true;
  uint64_t prev_ticket = ticket - 1;
  TicketSlot& slot = queues[prev_ticket % NUM_OFFSET_QUEUE_SLOT];
  while (slot.ticket_slot.ticket.load(std::memory_order_acquire) !=
         prev_ticket) {
    asm volatile("pause");
  }

  if (!file->tx_idx_greater(slot.ticket_slot.tail_tx_idx, curr_idx,
                            slot.ticket_slot.tail_tx_block, curr_block))
    return true;
  prev_idx = slot.ticket_slot.tail_tx_idx;
  prev_block = slot.ticket_slot.tail_tx_block;
  return false;
}

void OffsetMgr::release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                               const pmem::TxBlock* curr_block) {
  TicketSlot& slot = queues[ticket % NUM_OFFSET_QUEUE_SLOT];
  slot.ticket_slot.tail_tx_idx = curr_idx;
  slot.ticket_slot.tail_tx_block = curr_block;
  slot.ticket_slot.ticket.store(ticket, std::memory_order_release);
}

};  // namespace ulayfs::dram