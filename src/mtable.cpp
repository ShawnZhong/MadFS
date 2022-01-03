#include "mtable.h"

#include "file.h"

namespace ulayfs::dram {

MemTable::MemTable(int fd, off_t init_file_size, bool read_only) {
  this->fd = fd;

  PANIC_IF(!IS_ALIGNED(init_file_size, BLOCK_SIZE), "not block aligned");

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

  pmem::Block* blocks = mmap_file(file_size, 0, 0, read_only);
  meta = &blocks->meta_block;
  if (!is_empty && !meta->is_valid())
    throw FileInitException("invalid meta block");

  // compute number of blocks and update the mata block if necessary
  auto num_blocks = BLOCK_SIZE_TO_IDX(file_size);
  if (should_grow) meta->set_num_blocks_if_larger(num_blocks);

  // initialize the mapping
  for (LogicalBlockIdx idx = 0; idx < num_blocks; idx += NUM_BLOCKS_PER_GROW)
    table.emplace(idx, blocks + idx);
}

std::ostream& operator<<(std::ostream& out, const MemTable& m) {
  out << "MemTable:\n";
  for (const auto& [blk_idx, mem_addr] : m.table) {
    out << "\t" << blk_idx << " - " << blk_idx + NUM_BLOCKS_PER_GROW << ": ";
    out << mem_addr << " - " << mem_addr + NUM_BLOCKS_PER_GROW << "\n";
  }
  return out;
}

}  // namespace ulayfs::dram
