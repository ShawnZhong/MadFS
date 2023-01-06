The [`Tx`](tx.h) class represents a single ongoing transaction. It has two
subclasses:

- [`ReadTx`](read.h) is for read operations

- [`WriteTx`](write.h) is for write operations, with the following two
  subclasses:

    - [`AlignedTx`](write_aligned.h) is for aligned writes, which only require
      DRAM-to-PMEM copy

    - [`CoWTx`](write_unaligned.h) is for unaligned write, and thus requires
      copy-on-write. It has two subclasses, `SingleBlockTx` if the writes is
      within a single block, and `MultiBlockTx` if the writes is across multiple
      blocks.
