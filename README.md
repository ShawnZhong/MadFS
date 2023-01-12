# uLayFS

[![workflow](https://github.com/shawnzhong/uLayFS/actions/workflows/test.yml/badge.svg)](https://github.com/ShawnZhong/uLayFS/actions/workflows/test.yml)
[![workflow](https://github.com/shawnzhong/uLayFS/actions/workflows/bench.yml/badge.svg)](https://github.com/ShawnZhong/uLayFS/actions/workflows/bench.yml)

## Prerequisites

- uLayFS is developed on Ubuntu 20.04.3 LTS (with Linux kernel 5.4)

- We recommend using Python 3.8.10, CMake 3.16.3, GCC 10.3.0 (or 9.4.0), and
  Clang 10.0.0, but other versions should also work.

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

    - Initialize namespaces (optional)
      ```shell
      sudo ndctl destroy-namespace all --region=region0 --force # remove existing namespaces
      sudo ndctl create-namespace --region=region0 --size=20G   # create new namespace
      ndctl list --region=0 --namespaces --human --idle         # list namespaces
      ```

    - Use `/dev/pmem0` to mount ext4-DAX at `/mnt/pmem0-ext4-dax`
      ```shell
      sudo mkfs.ext4 /dev/pmem0               # create filesystem
      sudo mkdir -p /mnt/pmem0-ext4-dax       # create mount point
      sudo mount -o dax /dev/pmem0 /mnt/pmem0-ext4-dax # mount filesystem
      sudo chmod a+w /mnt/pmem0-ext4-dax      # make the mount point writable
      mount -v | grep /mnt/pmem0-ext4-dax     # check mount status
      ```

    - Use `/dev/pmem0.1` to mount NOVA at `/mnt/pmem0-nova` (optional)
      ```shell
      sudo modprobe nova                       # load NOVA module
      sudo mkdir -p /mnt/pmem0-nova            # create mount point
      sudo mount -t NOVA -o init -o data_cow  /dev/pmem0.1 /mnt/pmem0-nova # mount filesystem
      sudo chmod a+w /mnt/pmem0-nova           # make the mount point writable
      mount -v | grep /mnt/pmem0-nova          # check mount status
      ```

    - To unmount the filesystems, run
      ```shell
      sudo umount /mnt/pmem0-ext4-dax
      sudo umount /mnt/pmem0-nova
      ```
  </details>

## Build and Run

- Build the uLayFS shared library

  ```shell
  make BUILD_TARGETS="ulayfs"
  ```

- Run your program with uLayFS

  ```shell
  LD_PRELOAD=./build-release/libulayfs.so ./your_program
  ```

## Development

- Build

  ```shell
  # usage: make [build_type] 
  #             [CMAKE_ARGS="-DKEY1=VAL1 -DKEY2=VAL2 ..."] 
  #             [BUILD_TARGETS="target1 target2 ..."] 
  #             [BUILD_ARGS="..."]
  
  # build the uLayFS shared library and tests
  make
  ```

- Build and run a single test suite or benchmark suite

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

 
