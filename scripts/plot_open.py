#!/usr/bin/env python3
import re
from argparse import ArgumentParser
from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from plot_utils import export_df, save_fig, get_latest_result
from utils import root_dir


def parse_line(line):
    # OPEN_SYS                 :       1 (  14.079 us,   0.01 ms)
    m = re.match(
        r"(?P<name>\S+).*:"
        r"\s+(?P<calls>\d+)\s+"
        r"\((?P<avg>.*) us,.*\s+(?P<total>\d+\.\d+) ms\)",
        line,
    )
    if m is None:
        return None

    return m.groupdict()


def parse_file(path):
    result = []
    with open(path) as f:
        for line in f:
            data = parse_line(line.strip())
            if data is not None:
                result.append(data)

    return pd.DataFrame(result)


def parse_results(result_dir):
    result = []
    for f in result_dir.iterdir():
        if f.name.endswith("_open.log"):
            df = parse_file(f)
            df["size"] = int(f.name.split("M")[0])
            result.append(df)
    return pd.concat(result)


def plot_open(result_dir):
    df = parse_results(result_dir)
    export_df(result_dir, df)

    df = df.pivot(index="size", columns="name", values="avg").reset_index()
    export_df(result_dir, df, "pivot")

    df = df.apply(pd.to_numeric)

    coeff = np.polyfit(df["size"] / 2, df["MMAP"], 1)
    print(f"MMAP = {coeff[0]:.3f} us per 2MB + {coeff[1]:.3f} us")

    df = df[df["size"].isin([4, 16, 64, 256])]
    df["Others"] = df["OPEN"] - df["MMAP"] - df["UPDATE"]

    columns = {
        "MMAP": "Mmap",
        "UPDATE": "Block Table",
    }
    df.rename(columns=columns, inplace=True)
    df.sort_values(by="size", inplace=True, ascending=False)

    ax = df.plot.barh(
        x="size",
        y=["Mmap", "Block Table", "Others"],
        stacked=True,
        legend=False,
        figsize=(5, 1),
    )

    ax.set_ylabel("File Size (MB)    ")
    ax.set_xlabel(r"Time ($\mu$s)")
    ax.legend(
        ncols=3,
        fontsize=9,
        columnspacing=.75,
        handlelength=0.75,
        handletextpad=0.3,
        loc="upper right",
    )
    _, xmax = ax.get_xlim()
    xmax = (int(xmax / 1000) + 1) * 1000
    tick_size = 1000
    ax.set_xlim([0, xmax])
    ax.xaxis.set_major_locator(plt.MultipleLocator(tick_size))

    save_fig(ax.get_figure(), "result", result_dir)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-r", "--result_dir", help="Directory with results", type=Path,
                        default=get_latest_result(root_dir / "results" / "bench_open" / "exp"))
    args = parser.parse_args()
    plot_open(args.result_dir)
