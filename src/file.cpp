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

File::~File() { mem_table.unmap(); }

ssize_t File::pwrite(const void* buf, size_t count, size_t offset) {
  if (count == 0) return 0;
  blk_table.update();
  tx_mgr.do_cow(buf, count, offset);
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

    const char* ptr = get_ro_data_ptr(virtual_idx + i);
    const char* src = i == 0 ? ptr + local_offset : ptr;

    memcpy(dst, src, num_bytes);
    dst += num_bytes;
  }

  return static_cast<ssize_t>(count);
}

off_t File::lseek(off_t offset, int whence) {
  off_t old_off = file_offset;

  switch (whence) {
    case SEEK_SET: {
      if (offset < 0) return -1;
      __atomic_store_n(&file_offset, offset, __ATOMIC_RELEASE);
      return file_offset;
    }

    case SEEK_CUR: {
      off_t new_off;
      do {
        new_off = old_off + offset;
        if (new_off < 0) return -1;
      } while (!__atomic_compare_exchange_n(&file_offset, &old_off, new_off,
                                            true, __ATOMIC_ACQ_REL,
                                            __ATOMIC_RELAXED));
      return file_offset;
    }

    case SEEK_END:
      // TODO: enable this code after file_size is implemented
      // new_off = meta->get_file_size() + offset;
      // break;

      // TODO: add SEEK_DATA and SEEK_HOLE
    case SEEK_DATA:
    case SEEK_HOLE:
    default:
      return -1;
  }
}

ssize_t File::write(const void* buf, size_t count) {
  // atomically add then return the old value
  off_t old_off = __atomic_fetch_add(&file_offset, static_cast<off_t>(count),
                                     __ATOMIC_ACQ_REL);

  return pwrite(buf, count, old_off);
}

ssize_t File::read(void* buf, size_t count) {
  off_t new_off;
  off_t old_off = file_offset;

  do {
    // TODO: place file_offset to EOF when entire file is read
    new_off = old_off + static_cast<off_t>(count);
  } while (!__atomic_compare_exchange_n(&file_offset, &old_off, new_off, true,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));

  return pread(buf, count, old_off);
}

const char* File::get_ro_data_ptr(VirtualBlockIdx virtual_block_idx) {
  static char empty_block[BLOCK_SIZE]{};
  auto logical_block_idx = blk_table.get(virtual_block_idx);
  if (logical_block_idx == 0) {
    INFO("Virtual block %d is a hole block", virtual_block_idx);
    return empty_block;
  }
  auto block = mem_table.get_addr(logical_block_idx);
  return block->data;
}

std::ostream& operator<<(std::ostream& out, const File& f) {
  out << "File: fd = " << f.fd << ", offset = " << f.file_offset << "\n";
  out << *f.meta;
  out << f.mem_table;
  out << f.tx_mgr;
  out << f.blk_table;
  out << "\n";

  return out;
}

}  // namespace ulayfs::dram
