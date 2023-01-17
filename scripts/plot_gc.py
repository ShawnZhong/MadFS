#!/usr/bin/env python3
from argparse import ArgumentParser
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt

from plot_utils import save_fig
from utils import root_dir, get_latest_result


def plot_cdf(x, bins):
    hist, bins = np.histogram(x, bins=bins)
    logbins = np.logspace(np.log10(bins[0]), np.log10(bins[-1]), len(bins))
    plt.hist(x, bins=logbins, cumulative=True, density=True)
    plt.xscale('log')


def plot_file(file):
    print(file)
    data = np.fromfile(file, dtype=np.uint32)
    for p in [0, 25, 50, 75, 99, 99.9, 99.99, 99.999, 99.9999]:
        print(f"{p:8}th percentile: {np.percentile(data, p) / 1e3:.3f} us")
    plt.clf()
    plot_cdf(data, 1000)
    save_fig(plt.gcf(), file.name, file.parent)
    print()


def plot_gc(result_dir):
    for name in ["gc", "io"]:
        path = result_dir / name
        for file in sorted(path.iterdir()):
            if file.suffix == "":
                plot_file(file)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-r", "--result_dir", help="Directory with results", type=Path,
                        default=get_latest_result(root_dir / "results" / "micro_gc" / "exp"))
    args = parser.parse_args()
    plot_gc(args.result_dir)
