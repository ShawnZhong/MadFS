#pragma once

#include "const.h"
#include "entry.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace madfs::pmem {

// LogEntryBlock is per-thread to avoid contention
class LogEntryBlock : public noncopyable {
  // a LogEntryBlock is essentially a lightweight heap for transactions
  // the major abstraction is really just a byte array
  char pool[BLOCK_SIZE];

 public:
  [[nodiscard]] LogEntry* get(LogLocalOffset offset) {
    return reinterpret_cast<LogEntry*>(&pool[offset]);
  }
};

static_assert(sizeof(LogEntryBlock) == BLOCK_SIZE,
              "LogEntryBlock must be of size BLOCK_SIZE");

}  // namespace madfs::pmem
