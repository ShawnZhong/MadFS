#!/usr/bin/env python3

import psutil

from utils import system

install_dev_deps_cmds = """
sudo apt install -y clang libstdc++-10-dev clang-format &&    # for sanitizers and formatter
sudo apt install -y linux-tools-common linux-tools-generic linux-tools-`uname -r` && # for perf
sudo apt install -y ndctl numactl &&                          # for managing persistent memory and NUMA
sudo apt install -y sqlite3                                   # for benchmarking
"""

configure_cmds = """
sudo sysctl -w vm.nr_hugepages=512 &&               # enable huge pages
sudo sysctl -w kernel.kptr_restrict=0 &&            # expose kernel addresses for perf
sudo sysctl -w kernel.perf_event_paranoid=-1 &&     # allow kernel profiling
sudo sysctl -w vm.max_map_count=131072              # increase max number of mmap regions
"""

configure_cmds += " &&\n".join(
    f"echo performance | sudo tee /sys/devices/system/cpu/cpu{i}/cpufreq/scaling_governor >/dev/null"
    for i in range(psutil.cpu_count())
)


def init(install_build_deps=False, install_dev_deps=False, configure=False):
    if install_build_deps or install_dev_deps:
        system("sudo apt update")
    if install_build_deps:
        system("sudo apt install -y cmake build-essential")
    if install_dev_deps:
        system(install_dev_deps_cmds)
    if configure:
        system(configure_cmds)
