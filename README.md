# uLayFS

[![workflow](https://github.com/shawnzhong/uLayFS/actions/workflows/default.yml/badge.svg)](https://github.com/ShawnZhong/uLayFS/actions/workflows/default.yml)

## Getting Started

uLayFS is developed on Ubuntu 20.04.3 LTS (with Linux kernel 5.4).

### Prerequisites

- Install dependencies

    ```shell
    ./scripts/init --install_build_deps
    ./scripts/init --install_dev_deps # optional
    ```

- Configure the system

    ```shell
    # may need to run this everytime after a reboot
    ./scripts/init --configure
    ```

- To emulate a persistent memory device using DRAM, follow the
  guide [here](https://docs.pmem.io/persistent-memory/getting-started-guide/creating-development-environments/linux-environments/linux-memmap)
  .

- Configure persistent memory

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

### Build and Run

- Clone

  ```shell
  git clone git@github.com:ShawnZhong/uLayFS.git
  cd uLayFS
  git submodule update --init --recursive
  ```

- Build the uLayFS shared library
  ```shell
  # usage: make [build_type] 
  #             [CMAKE_ARGS="-DKEY1=VAL1 -DKEY2=VAL2 ..."] 
  #             [BUILD_TARGETS="target1 target2 ..."] 
  #             [BUILD_ARGS="..."]
  make release BUILD_TARGETS="ulayfs"
  ```

- Run your program with uLayFS

  ```shell
  LD_PRELOAD=./build-release/libulayfs.so ./your_program
  ```

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
  ./run bench_append profile --prog_args="--benchmark_filter='bench/4096'"
  
  # profile append benchmark with kernel filesystem
  ./run bench_append profile --disable_ulayfs
  ```
