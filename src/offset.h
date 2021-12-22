#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

#include "block.h"
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

    TicketSlot() { memset(&ticket_slot, 0, sizeof(ticket_slot)); }
    TicketSlot(TicketSlot const&) = delete;
    TicketSlot(TicketSlot&&) = delete;
    TicketSlot& operator=(TicketSlot const&) = delete;
    TicketSlot& operator=(TicketSlot&&) = delete;
  };

  File* file;
  int64_t offset;
  uint64_t next_ticket;
  TicketSlot queues[NUM_OFFSET_QUEUE_SLOT];

 public:
  OffsetMgr(File* file, int64_t offset)
      : file(file), offset(offset), next_ticket(0), queues() {}

  // must have btable spinlock acquired
  int64_t acquire_offset(int64_t change, int64_t file_size, bool check_boundary,
                         bool error_if_out, uint64_t& ticket);

  bool validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                       const pmem::TxBlock* curr_block, TxEntryIdx& prev_idx,
                       const pmem::TxBlock*& prev_block);

  void release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                      const pmem::TxBlock* curr_block);
};

};  // namespace ulayfs::dram
