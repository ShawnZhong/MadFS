#include "tx/read.h"

#include "file/file.h"

namespace ulayfs::dram {
ssize_t File::pread(char* buf, size_t count, size_t offset) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  TimerGuard<Event::READ_TX> timer_guard;
  timer.start<Event::READ_TX_CTOR>();
  return ReadTx(this, buf, count, offset).exec();
}

ssize_t File::read(char* buf, size_t count) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;

  FileState state;
  uint64_t ticket;
  uint64_t offset;
  blk_table.update([&](const FileState& file_state) {
    offset = offset_mgr.acquire(count, file_state.file_size,
                                /*stop_at_boundary*/ true, ticket);
    state = file_state;
  });

  return Tx::exec_and_release_offset<ReadTx>(this, buf, count, offset, state,
                                             ticket);
}
}  // namespace ulayfs::dram
