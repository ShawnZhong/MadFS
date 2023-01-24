#!/usr/bin/env python3

import os
from pathlib import Path

from scripts.utils import system


def init():
    cmds = ""

    # enable huge pages
    if Path("/proc/sys/vm/nr_hugepages").read_text() != "512\n":
        cmds += "sudo sysctl -w vm.nr_hugepages=512\n"

    # expose kernel addresses for perf
    if Path("/proc/sys/kernel/kptr_restrict").read_text() != "0\n":
        cmds += "sudo sysctl -w kernel.kptr_restrict=0\n"

    # allow kernel profiling
    if Path("/proc/sys/kernel/perf_event_paranoid").read_text() != "-1\n":
        cmds += "sudo sysctl -w kernel.perf_event_paranoid=-1\n"

    # increase max number of mmap regions
    if Path("/proc/sys/vm/max_map_count").read_text() != "131072\n":
        cmds += "sudo sysctl -w vm.max_map_count=131072\n"

    # disable CPU frequency scaling
    paths = [
        Path(f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_governor")
        for i in range(os.cpu_count())
    ]
    cmds += " &&\n".join(
        f"echo performance | sudo tee {path} >/dev/null"
        for path in paths
        if path.read_text() != "performance\n"
    )

    if cmds != "":
        system(cmds)


if __name__ == "__main__":
    init()
