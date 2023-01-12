Single-threaded benchmarks

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
./scripts/bench_st -f uLayFS SplitFS

# Profile a data point
./scripts/bench_st --filter="seq_pread/512" -f uLayFS -b profile
```
