#include "file.h"

namespace ulayfs::dram {

File::File(const char* pathname, int flags, mode_t mode)
    : open_flags(flags), valid(false) {
  if ((flags & O_ACCMODE) == O_WRONLY) {
    INFO("File \"%s\" opened with O_WRONLY. Changed to O_RDWR.", pathname);
    flags &= ~O_WRONLY;
    flags |= O_RDWR;
  }

  fd = posix::open(pathname, flags, mode);
  if (fd < 0) return;  // fail to open the file

  // TODO: support read-only files
  if ((flags & O_ACCMODE) == O_RDONLY) {
    WARN("File \"%s\" opened with O_RDONLY. Fallback to syscall.", pathname);
    return;
  }

  struct stat stat_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  int ret = posix::fstat(fd, &stat_buf);
  PANIC_IF(ret, "fstat failed");

  // we don't handle non-normal file (e.g., socket, directory, block dev)
  if (!S_ISREG(stat_buf.st_mode) && !S_ISLNK(stat_buf.st_mode)) {
    WARN("Unable to handle non-normal file \"%s\"", pathname);
    return;
  }

  if (!IS_ALIGNED(stat_buf.st_size, BLOCK_SIZE)) {
    WARN("File size not aligned for \"%s\". Fall back to syscall", pathname);
    return;
  }

  mem_table = MemTable(fd, stat_buf.st_size);
  meta = mem_table.get_meta();
  allocator = Allocator(fd, meta, &mem_table);
  log_mgr = LogMgr(meta, &allocator, &mem_table);
  tx_mgr = TxMgr(meta, &allocator, &mem_table, &log_mgr, &blk_table);
  blk_table = BlkTable(meta, &mem_table, &log_mgr, &tx_mgr);
  blk_table.update();

  if (stat_buf.st_size == 0) meta->init();

  valid = true;
}

ssize_t File::pwrite(const void* buf, size_t count, size_t offset) {
  if (count == 0) return 0;
  tx_mgr.do_cow(buf, count, offset);
  blk_table.update();
  return static_cast<ssize_t>(count);
}

ssize_t File::pread(void* buf, size_t count, off_t offset) {
  VirtualBlockIdx virtual_idx = offset >> BLOCK_SHIFT;

  uint64_t local_offset = offset - virtual_idx * BLOCK_SIZE;
  uint32_t num_blocks =
      ALIGN_UP(count + local_offset, BLOCK_SIZE) >> BLOCK_SHIFT;
  uint16_t last_remaining = num_blocks * BLOCK_SIZE - count - local_offset;

  char* dst = static_cast<char*>(buf);
  for (size_t i = 0; i < num_blocks; ++i) {
    size_t num_bytes = BLOCK_SIZE;
    if (i == 0) num_bytes -= local_offset;
    if (i == num_blocks - 1) num_bytes -= last_remaining;

    char* ptr = get_data_block_ptr(virtual_idx + i);
    char* src = i == 0 ? ptr + local_offset : ptr;

    memcpy(dst, src, num_bytes);
    dst += num_bytes;
  }

  return static_cast<ssize_t>(count);
}

char* File::get_data_block_ptr(VirtualBlockIdx virtual_block_idx) {
  auto logical_block_idx = blk_table.get(virtual_block_idx);
  assert(logical_block_idx != 0);
  auto block = mem_table.get_addr(logical_block_idx);
  return block->data;
}

std::ostream& operator<<(std::ostream& out, const File& f) {
  out << "File: fd = " << f.fd << "\n";
  out << *f.meta;
  out << f.mem_table;
  out << f.tx_mgr;
  out << f.blk_table;
  out << "\n";

  return out;
}

}  // namespace ulayfs::dram
