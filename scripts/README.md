- Single-threaded benchmarks

    ```shell
    # All the benchmarks in the suite
    ./scripts/bench_st
    
    # Filter out benchmarks by name
    ./scripts/bench_st --filter="seq_pread"
    ./scripts/bench_st --filter="rnd_pread"
    ./scripts/bench_st --filter="seq_pwrite"
    ./scripts/bench_st --filter="rnd_pwrite"
    ./scripts/bench_st --filter="cow"
    ./scripts/bench_st --filter="append_pwrite"
    
    # Limit to set of file systems
    ./scripts/bench_st -f MadFS SplitFS
    
    # Profile a data point
    ./scripts/bench_st --filter="seq_pread/512" -f MadFS -b profile
    ```

- Multi-threaded benchmarks

    ```shell
    ./scripts/bench_mt --filter="unif_0R"
    ./scripts/bench_mt --filter="unif_50R"
    ./scripts/bench_mt --filter="unif_95R"
    ./scripts/bench_mt --filter="unif_100R"
    ./scripts/bench_mt --filter="zipf_2k"
    ./scripts/bench_mt --filter="zipf_4k"
    ```
