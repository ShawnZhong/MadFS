This folder contains code for allocation.

The main entry point is the `alloc.h` file, which exposes `class Allocator`.
This class is a per-thread data structure that is constructed and retrieved by
`File::get_or_create_allocator()`.

The class contains the following public members:

- `class BlockAllocator` is a block allocator that allocates blocks of a fixed
  size less than or equal to 64 blocks.

- `class TxBlockAllocator` allocates transaction blocks. It also keeps track of
  the currently using tx block in the shared memory. It depends on the
  `BlockAllocator` class to allocate blocks.

- `class LogEntryAllocator` allocates log entries. It also depends on the
  `BlockAllocator` class to allocate blocks.
