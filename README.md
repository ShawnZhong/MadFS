# uLayFS

[![workflow](https://github.com/shawnzhong/uLayFS/actions/workflows/default.yml/badge.svg)](https://github.com/ShawnZhong/uLayFS/actions/workflows/default.yml)

## Getting Started

uLayFS is developed on Ubuntu 20.04.3 LTS (with Linux kernel 5.4).

### Prerequisites

- Install dependencies

    ```shell
    # build dependencies
    sudo apt update
    sudo apt install -y cmake build-essential
    
    # dev dependencies (optional)
    sudo apt install -y clang clang-tidy libstdc++-10-dev # for sanitizers
    sudo apt install -y linux-tools-common linux-tools-generic linux-tools-`uname -r` # for perf
    sudo apt install -y ndctl
    ```

- To emulate a persistent memory device, follow the
  guide [here](https://docs.pmem.io/persistent-memory/getting-started-guide/creating-development-environments/linux-environments/linux-memmap)

- Configure persistent memory

    ```shell
    # replace pmem0 with the name of your pmem device
    PMEM="pmem0"
  
    # create ext4 filesystem
    sudo mkfs.ext4 /dev/${PMEM}
  
    # mount the filesystem
    sudo mkdir -p /mnt/${PMEM}
    sudo mount -o dax /dev/${PMEM} /mnt/${PMEM}
  
    # verify mount
    sudo mount -v | grep /mnt/${PMEM}
  
    # set permission
    sudo chown -R $USER:$GROUP /mnt/${PMEM}
    ```

- Additional system configutations (optional)

    ```shell
    # to collect kernel traces in `perf`
    sudo sysctl -w kernel.kptr_restrict=0
    sudo sysctl -w kernel.perf_event_paranoid=-1
  
    # enable huge page
    sudo sysctl -w vm.nr_hugepages=512
  
    # for consistent benchmark result
    sudo cpupower frequency-set --governor performance
    ```

### Build and Run

- Clone

  ```shell
  git clone git@github.com:ShawnZhong/uLayFS.git
  cd uLayFS
  git submodule update --init --recursive
  ```

- Build (optional)
  ```shell
  # usage: make [build_mode] [CMAKE_ARGS="-DKEY=VAL"] [BUILD_ARGS="TARGETS"]
  make release
  ```

- Build and run tests and benchmarks

  ```shell
  # usage: see `./run --help`
  ./run test_basic
  ./run test_sync tsan
  ./run test_rw pmemcheck -c="-DTX_FLUSH_ONLY_FSYNC=ON"
  ./run bench_rw profile -p="--benchmark_filter=bench_rw/4096"
  ```
