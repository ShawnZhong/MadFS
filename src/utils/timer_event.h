#pragma once

#include <cstddef>

namespace ulayfs {
enum class Event : std::size_t {
  READ,
  WRITE,
  PREAD,
  PWRITE,
  OPEN_SYS,
  MMAP,
  CLOSE,
  FSYNC,

  UPDATE,

  READ_TX,
  READ_TX_CTOR,
  READ_TX_UPDATE,
  READ_TX_COPY,
  READ_TX_VALIDATE,

  ALIGNED_TX,
  ALIGNED_TX_CTOR,
  ALIGNED_TX_EXEC,
  ALIGNED_TX_COPY,
  ALIGNED_TX_PREPARE,
  ALIGNED_TX_UPDATE,
  ALIGNED_TX_RECYCLE,
  ALIGNED_TX_WAIT_OFFSET,
  ALIGNED_TX_COMMIT,
  ALIGNED_TX_FREE,

  SINGLE_BLOCK_TX,
  SINGLE_BLOCK_TX_START,
  SINGLE_BLOCK_TX_COPY,
  SINGLE_BLOCK_TX_COMMIT,

  MULTI_BLOCK_TX,
  MULTI_BLOCK_TX_START,
  MULTI_BLOCK_TX_COPY,
  MULTI_BLOCK_TX_COMMIT,

  TX_ENTRY_LOAD,
  TX_ENTRY_STORE,

  GC_CREATE,
};

// A list of events defined but not accounted for:
static constexpr Event disabled_events[] = {
    Event::READ_TX_CTOR,       Event::ALIGNED_TX_CTOR,
    Event::ALIGNED_TX_EXEC,    Event::ALIGNED_TX_PREPARE,
    Event::ALIGNED_TX_RECYCLE, Event::ALIGNED_TX_WAIT_OFFSET,
    Event::ALIGNED_TX_FREE,    Event::TX_ENTRY_LOAD,
    Event::TX_ENTRY_STORE,
};

}  // namespace ulayfs
