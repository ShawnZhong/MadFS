#!/usr/bin/env python3
from argparse import ArgumentParser
from pathlib import Path

import numpy as np

from utils import root_dir, get_latest_result


def read_data(result_dir):
    for name in ["gc", "io"]:
        path = result_dir / name
        for file in sorted(path.iterdir()):
            print(file)
            data = np.fromfile(file, dtype=np.uint32)
            for p in [50, 99, 99.9, 99.99, 99.999, 99.9999]:
                print(f"{p:8}th percentile: {np.percentile(data, p) / 1e3:.3f} us")
            print()


def plot_gc(result_dir):
    read_data(result_dir)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--result_dir", help="Directory with results", type=Path,
                        default=get_latest_result(root_dir / "results" / f"bench_micro_gc"))
    args = parser.parse_args()
    plot_gc(args.result_dir)
