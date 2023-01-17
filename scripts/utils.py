import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("utils")

root_dir = Path(__file__).parent.parent


def init(install_build_deps=False, install_dev_deps=False, configure=False):
    install_dev_deps_cmds = """
sudo apt install -y clang-10 libstdc++-10-dev clang-format-10 &&    # for sanitizers and formatter
sudo apt install -y linux-tools-common linux-tools-generic linux-tools-`uname -r` && # for perf
sudo apt install -y ndctl numactl &&                                # for managing persistent memory and NUMA
sudo apt install -y sqlite3                                         # for benchmarking
"""

    configure_cmds = ""

    # enable huge pages
    if Path("/proc/sys/vm/nr_hugepages").read_text() != "512\n":
        configure_cmds += "sudo sysctl -w vm.nr_hugepages=512\n"

    # expose kernel addresses for perf
    if Path("/proc/sys/kernel/kptr_restrict").read_text() != "0\n":
        configure_cmds += "sudo sysctl -w kernel.kptr_restrict=0\n"

    # allow kernel profiling
    if Path("/proc/sys/kernel/perf_event_paranoid").read_text() != "-1\n":
        configure_cmds += "sudo sysctl -w kernel.perf_event_paranoid=-1\n"

    # increase max number of mmap regions
    if Path("/proc/sys/vm/max_map_count").read_text() != "131072\n":
        configure_cmds += "sudo sysctl -w vm.max_map_count=131072\n"

    # disable CPU frequency scaling
    paths = [
        Path(f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_governor")
        for i in range(os.cpu_count())
    ]
    configure_cmds += " &&\n".join(
        f"echo performance | sudo tee {path} >/dev/null"
        for path in paths
        if path.read_text() != "performance\n"
    )

    if install_build_deps or install_dev_deps:
        system("sudo apt update")
    if install_build_deps:
        system("sudo apt install -y cmake build-essential gcc-10 g++-10")
    if install_dev_deps:
        system(install_dev_deps_cmds)
    if configure and configure_cmds != "":
        system(configure_cmds)


def is_madfs_linked(prog_path: Path):
    import subprocess
    import re

    output = subprocess.check_output(["ldd", shutil.which(prog_path)]).decode("utf-8")
    for line in output.splitlines():
        match = re.match(r"\t(.*) => (.*) \(0x", line)
        if match and match.group(1) == "libmadfs.so":
            logger.info(
                f"`{prog_path}` is already linked with MadFS ({match.group(2)})"
            )
            return True
    logger.info(
        f"`{prog_path}` is not linked with MadFS by default. Need to run with `env LD_PRELOAD=...`"
    )
    return False


def system(cmd, log_path=None):
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = f"({cmd}) 2>&1 | tee -a {log_path}"

    cmd = f'/bin/bash -o pipefail -c "{cmd}"'
    msg = f"Executing command: {cmd}"
    logger.info(msg)
    if log_path:
        with log_path.open("a") as f:
            f.write(msg + "\n")
    ret = os.system(cmd)
    if ret != 0:
        sys.exit(f"Command `{cmd}` failed with return code {ret}")


def get_latest_result(path):
    return max(path.glob("*"), default=None)


def get_timestamp():
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def get_cpulist(numa_node: int) -> List[int]:
    result = []
    text = Path(f"/sys/devices/system/node/node{numa_node}/cpulist").read_text()
    for r in text.split(","):
        start, end = r.split("-")
        result += list(range(int(start), int(end) + 1))
    return result
