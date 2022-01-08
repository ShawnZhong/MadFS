# uLayFS

[![workflow](https://github.com/shawnzhong/uLayFS/actions/workflows/test.yml/badge.svg)](https://github.com/ShawnZhong/uLayFS/actions/workflows/test.yml)
[![workflow](https://github.com/shawnzhong/uLayFS/actions/workflows/bench.yml/badge.svg)](https://github.com/ShawnZhong/uLayFS/actions/workflows/bench.yml)

## Prerequisites

- uLayFS is developed on Ubuntu 20.04.3 LTS (with Linux kernel 5.4)

- Install dependencies and configure the system

    ```shell
    ./scripts/init --install_build_deps
    ./scripts/init --install_dev_deps # optional
    ./scripts/init --configure # run this after every reboot
    ```

- To emulate a persistent memory device using DRAM, please follow the
  guide [here][1].

  [1]: https://docs.pmem.io/persistent-memory/getting-started-guide/creating-development-environments/linux-environments/linux-memmap

- <details>
  <summary>Configure persistent memory</summary>

  ```shell
  # replace pmem0 with the name of your pmem device
  PMEM="pmem0"
  
  # create ext4 filesystem
  sudo mkfs.ext4 /dev/${PMEM}
  
  # mount the filesystem
  sudo mkdir -p /mnt/${PMEM} 
  sudo mount -o dax /dev/${PMEM} /mnt/${PMEM} 
  sudo mount -v | grep /mnt/${PMEM}
  
  # change permission
  sudo chmod a+w /mnt/${PMEM}
  ```

  </details>

## Build and Run

- Build the project

  ```shell
  # usage: make [build_type] 
  #             [CMAKE_ARGS="-DKEY1=VAL1 -DKEY2=VAL2 ..."] 
  #             [BUILD_TARGETS="target1 target2 ..."] 
  #             [BUILD_ARGS="..."]
  
  # build the uLayFS shared library and tests in debug mode
  make
  
  # build just the uLayFS shared library in release mode
  make release BUILD_TARGETS="ulayfs"
  ```

- Run your program with uLayFS

  ```shell
  LD_PRELOAD=./build-release/libulayfs.so ./your_program
  ```
- Run benchmarks

  ```shell
  ./scripts/bench_micro st       # single-threaded microbenchmark
  ./scripts/bench_micro mt       # multi-threaded microbenchmark
  ./scripts/bench_micro meta     # microbenchmark with metadata
  ./scripts/bench_ycsb           # YCSB benchmark
  ```

## Development

- Build and run tests or benchmarks

  ```shell
  # print help message
  ./run
  
  # run smoke test in debug mode
  ./run test_basic
  
  # run synchronization test with thread sanitizer
  ./run test_sync tsan
  
  # run read/write test with pmemcheck
  ./run test_rw pmemcheck --cmake_args="-DULAYFS_TX_FLUSH_ONLY_FSYNC=ON"
  
  # profile 4K append with uLayFS
  ./run micro_mt profile --prog_args="--benchmark_filter='append/4096'"
  
  # profile multithreaded microbenchmark with kernel filesystem
  ./run micro_mt profile --disable_ulayfs
  ```

- Environment variables
    - `ULAYFS_NO_SHOW_CONFIG`: if defined, disable showing configuration when
      the program starts

    - `ULAYFS_LOG_FILE`: redirect log output to a file

    - `ULAYFS_LOG_LEVEL`: set the numerical log level: 0 for printing all
      messages, 1 for printing debug messages and above (default), and 4 for
      suppressing everything. 

 
