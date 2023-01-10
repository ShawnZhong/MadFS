#!/usr/bin/env python3
from argparse import ArgumentParser
from pathlib import Path

from plot import read_files, save_fig


def plot_meta(result_dir):
    df = read_files(result_dir)

    df["Memory Map"] = df["mmap"] / df["iterations"] / 1e3
    df["Block Table"] = df["update"] / df["iterations"] / 1e3
    df["Total"] = df["cpu_time"] / 1e3
    df["Others"] = df["Total"] - df["Memory Map"] - df["Block Table"]
    df["File Size"] = df["file_size"].apply(lambda x: f"{x / 1024 / 1024:.0f} MB")

    df = df[df["File Size"].isin(["2 MB", "8 MB", "32 MB"])]

    ax = df.plot.barh(
        x="File Size",
        y=["Memory Map", "Block Table", "Others"],
        stacked=True,
        figsize=(5, 1),
        legend=False
    )
    ax.set_xlabel(r"Time ($\mu$s)")
    ax.legend(
        ncols=3,
        fontsize=9,
        columnspacing=.75,
        handlelength=0.75,
        handletextpad=0.3,
        loc="lower right",
    )

    save_fig(ax.get_figure(), "result", result_dir)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("result_dir", help="Directory with results", type=Path)
    args = parser.parse_args()
    plot_meta(args.result_dir)
