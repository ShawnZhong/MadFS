# MadFS

[![workflow](https://github.com/shawnzhong/MadFS/actions/workflows/test.yml/badge.svg)](https://github.com/ShawnZhong/MadFS/actions/workflows/test.yml)
[![workflow](https://github.com/shawnzhong/MadFS/actions/workflows/bench.yml/badge.svg)](https://github.com/ShawnZhong/MadFS/actions/workflows/bench.yml)
[![workflow](https://github.com/shawnzhong/MadFS/actions/workflows/format.yml/badge.svg)](https://github.com/ShawnZhong/MadFS/actions/workflows/format.yml)

## Prerequisites

- MadFS is developed on Ubuntu 20.04.3 LTS and Ubuntu 22.04.1 LTS. It should
  work on other Linux distributions as well.

- MadFS requires a C++ compiler with C++ 20 support. The compilers known to work
  includes GCC 11.3.0, GCC 10.3.0, Clang 14.0.0, and Clang
  10.0.0.

- <details>
  <summary>Install dependencies and configure the system</summary>

    - Install build dependencies

      ```shell
      sudo apt update
      sudo apt install -y cmake build-essential gcc-10 g++-10
      ```

    - Install development dependencies (optional)

      ```shell
      # to run sanitizers and formatter
      sudo apt install -y clang-10 libstdc++-10-dev clang-format-10
      # for perf
      sudo apt install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`
      # for managing persistent memory and NUMA
      sudo apt install -y ndctl numactl
      # for benchmarking
      sudo apt install -y sqlite3
      ```

    - Configure the system

      ```shell
      ./scripts/init.py
      ```
  </details>

- <details>
  <summary>Configure persistent memory</summary>

    - To emulate a persistent memory device using DRAM, please follow the
      guide [here](https://docs.pmem.io/persistent-memory/getting-started-guide/creating-development-environments/linux-environments/linux-memmap).

    - Initialize namespaces (optional)
      ```shell
      # remove existing namespaces on region0
      sudo ndctl destroy-namespace all --region=region0 --force 
      # create new namespace `/dev/pmem0` on region0
      sudo ndctl create-namespace --region=region0 --size=20G
      # create new namespace `/dev/pmem0.1` on region0 for NOVA (optional)
      sudo ndctl create-namespace --region=region0 --size=20G
      # list all namespaces
      ndctl list --region=0 --namespaces --human --idle
      ```

    - Use `/dev/pmem0` to mount ext4-DAX at `/mnt/pmem0-ext4-dax`
      ```shell
      # create filesystem
      sudo mkfs.ext4 /dev/pmem0
      # create mount point
      sudo mkdir -p /mnt/pmem0-ext4-dax
      # mount filesystem
      sudo mount -o dax /dev/pmem0 /mnt/pmem0-ext4-dax
      # make the mount point writable
      sudo chmod a+w /mnt/pmem0-ext4-dax
      # check mount status
      mount -v | grep /mnt/pmem0-ext4-dax
      ```

    - Use `/dev/pmem0.1` to mount NOVA at `/mnt/pmem0-nova` (optional)
      ```shell
      # load NOVA module
      sudo modprobe nova
      # create mount point
      sudo mkdir -p /mnt/pmem0-nova
      # mount filesystem
      sudo mount -t NOVA -o init -o data_cow  /dev/pmem0.1 /mnt/pmem0-nova
      # make the mount point writable
      sudo chmod a+w /mnt/pmem0-nova           
      # check mount status
      mount -v | grep /mnt/pmem0-nova          
      ```

    - To unmount the filesystems, run
      ```shell
      sudo umount /mnt/pmem0-ext4-dax
      sudo umount /mnt/pmem0-nova
      ```

  </details>

## Build and Run

- Build the MadFS shared library

  ```shell
  make BUILD_TARGETS="madfs"
  ```

- Run your program with MadFS

  ```shell
  LD_PRELOAD=./build-release/libmadfs.so ./your_program
  ```

## Development

- Build

  ```shell
  # usage: make [build_type] 
  #             [CMAKE_ARGS="-DKEY1=VAL1 -DKEY2=VAL2 ..."] 
  #             [BUILD_TARGETS="target1 target2 ..."] 
  #             [BUILD_ARGS="..."]
  
  # build the MadFS shared library and tests
  make
  ```
