#!/usr/bin/env python3
import logging

from bench_utils import drop_cache
from fs import MADFS
from plot_open import plot_open
from runner import Runner
from utils import root_dir, get_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench_open")

logical_to_virtual_size = {
    2: 2 * 1024 * 1024 - 4096 * 2,
    4: 4 * 1024 * 1024 - 4096 * 3,
    8: 8 * 1024 * 1024 - 4096 * 5,
    16: 16 * 1024 * 1024 - 4096 * 9,
    32: 32 * 1024 * 1024 - 4096 * 17,
    64: 64 * 1024 * 1024 - 4096 * 33,
    128: 128 * 1024 * 1024 - 4096 * 65,
    256: 256 * 1024 * 1024 - 4096 * 129,
    512: 512 * 1024 * 1024 - 4096 * 257,
    1024: 1024 * 1024 * 1024 - 4096 * 514,
}


def main():
    result_dir = root_dir / "results" / "bench_open" / "exp" / get_timestamp()
    runner = Runner("bench_open", result_dir=result_dir)
    runner.build(cmake_args="-DMADFS_TIMER=ON")

    data_path = MADFS().path / "test.txt"

    for logical_size, virtual_size in logical_to_virtual_size.items():
        runner.run(
            prog_args=["--prepare", "-f", data_path, "-s", virtual_size],
            prog_log_name=f"{logical_size}M_prepare.log",
        )
        drop_cache()
        runner.run(
            prog_args=["--open", "-f", data_path],
            prog_log_name=f"{logical_size}M_open.log",
        )

    plot_open(result_dir)


if __name__ == "__main__":
    main()
