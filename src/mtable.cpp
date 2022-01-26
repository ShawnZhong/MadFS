#include "mtable.h"

#include <cstdint>
#include <ostream>

#include "const.h"
#include "idx.h"

namespace ulayfs::dram {

MemTable::MemTable(int fd, off_t init_file_size, bool read_only)
    : fd(fd), prot(read_only ? PROT_READ : PROT_READ | PROT_WRITE) {
  bool is_empty = init_file_size == 0;
  // grow to multiple of grow_unit_size if the file is empty or the file
  // size is not grow_unit aligned
  bool should_grow = is_empty || !IS_ALIGNED(init_file_size, GROW_UNIT_SIZE);
  off_t file_size = init_file_size;
  if (should_grow) {
    file_size =
        is_empty ? PREALLOC_SIZE : ALIGN_UP(init_file_size, GROW_UNIT_SIZE);
    int ret = posix::fallocate(fd, 0, 0, file_size);
    PANIC_IF(ret < 0, "fallocate failed");
  }

  first_region = mmap_file(file_size, 0, 0);
  first_region_num_blocks = BLOCK_SIZE_TO_IDX(file_size);
  meta = &first_region[0].meta_block;
  if (!is_empty && !meta->is_valid())
    throw FileInitException("invalid meta block");

  // update the mata block if necessary
  if (should_grow) meta->set_num_blocks_if_larger(first_region_num_blocks);
}

std::ostream& operator<<(std::ostream& out, const MemTable& m) {
  out << "MemTable:\n";
  uint32_t chunk_idx = 0;
  for (const auto mem_addr : m.table) {
    LogicalBlockIdx chunk_begin_lidx = chunk_idx << GROW_UNIT_IN_BLOCK_SHIFT;
    out << "\t" << chunk_begin_lidx << " - "
        << chunk_begin_lidx + NUM_BLOCKS_PER_GROW << ": ";
    out << mem_addr << " - " << mem_addr + NUM_BLOCKS_PER_GROW << "\n";
    ++chunk_idx;
  }
  return out;
}

}  // namespace ulayfs::dram
