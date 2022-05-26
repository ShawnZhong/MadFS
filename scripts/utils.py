import datetime
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("utils")

root_dir = Path(__file__).parent.parent


def init(install_build_deps=False, install_dev_deps=False, configure=False):
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
        for i in range(os.cpu_count())
    )

    if install_build_deps or install_dev_deps:
        system("sudo apt update")
    if install_build_deps:
        system("sudo apt install -y cmake build-essential")
    if install_dev_deps:
        system(install_dev_deps_cmds)
    if configure:
        system(configure_cmds)


def drop_cache():
    system("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null")


def is_ulayfs_linked(prog_path):
    import subprocess
    import re

    output = subprocess.check_output(['ldd', prog_path]).decode("utf-8")
    for line in output.splitlines():
        match = re.match(r'\t(.*) => (.*) \(0x', line)
        if match and match.group(1) == 'libulayfs.so':
            logger.info(f"`{prog_path}` is already linked with uLayFS ({match.group(2)})")
            return True
    logger.info(f"`{prog_path}` is not linked with uLayFS by default. Need to run with `env LD_PRELOAD=...`")
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


def get_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def add_common_args(argparser):
    from runner import build_types
    argparser.add_argument(
        "-b",
        "--build_type",
        choices=build_types,
    )
    argparser.add_argument(
        "additional_args",
        nargs="*",
        help="additional arguments to be passed to the program during execution",
    )
