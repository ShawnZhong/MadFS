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

  // must have spinlock acquired
  int64_t acquire_offset(int64_t change, int64_t file_size,
                         bool stop_at_boundary, uint64_t& ticket);

  // thread-safe
  bool wait_offset(uint64_t ticket, TxEntryIdx& prev_idx,
                   const pmem::TxBlock*& prev_block);

  // thread-safe
  bool validate_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                       const pmem::TxBlock* curr_block, TxEntryIdx& prev_idx,
                       const pmem::TxBlock*& prev_block);

  // thread-safe
  void release_offset(uint64_t ticket, const TxEntryIdx curr_idx,
                      const pmem::TxBlock* curr_block);

  friend std::ostream& operator<<(std::ostream& out, const OffsetMgr& o);
};

};  // namespace ulayfs::dram
