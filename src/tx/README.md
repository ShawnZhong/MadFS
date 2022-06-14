This folder exposes the transaction manager (`TxMgr`) interface defined in the
file [`mgr.h`](mgr.h).

The `Tx` class represents a single ongoing transaction. It has two subclasses:

- `ReadTx` is for read operations

- `WriteTx` is for write operations, with the following two subclasses:

    - `AlignedTx` is for aligned writes, which only require DRAM-to-PMEM copy

    - `CoWTx` is for unaligned write, and thus requires copy-on-write. It has
      two subclasses, `SingleBlockTx` if the writes is within a single block,
      and `MultiBlockTx` if the writes is across multiple blocks.
