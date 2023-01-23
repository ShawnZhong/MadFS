#!/usr/bin/env python3

import logging
from argparse import ArgumentParser
from pathlib import Path

from matplotlib import pyplot as plt

from plot_utils import read_files, parse_name, export_results, plot_single_bm
from utils import get_latest_result, root_dir

logger = logging.getLogger("plot_st")


def plot_st(result_dir):
    benchmarks = read_files(result_dir)
    benchmarks["benchmark"] = benchmarks["name"].apply(parse_name, args=(0,))

    for name, df in benchmarks.groupby("benchmark"):
        is_cow = name.startswith("cow")
        if is_cow:
            df["x"] = df["name"].apply(parse_name, args=(1,))
            df["y"] = df["items_per_second"].apply(lambda x: float(x) / 1000 ** 2)
            xunit = "Bytes"
            ylabel = r"Throughput (Mops/s)"
        else:
            df["x"] = (
                df["name"]
                .apply(parse_name, args=(1,))
                .apply(lambda x: f"{int(x) / 1024:1g}")
            )
            df["y"] = df["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)
            xunit = "KB"
            ylabel = "Throughput (GB/s)"

        export_results(result_dir, df, name=name)

        xlabel = f"Size ({xunit})"

        def post_plot(ax, **kwargs):
            ax.set_xlabel(xlabel, labelpad=0)
            ax.set_ylabel(ylabel, labelpad=0)

            if is_cow:
                ax.xaxis.set_major_locator(plt.MaxNLocator(4))
            elif "read" in name:
                ax.xaxis.set_major_locator(plt.MultipleLocator(2))

            _, ymax = ax.get_ylim()
            if ymax <= 1:
                tick_size = 0.2
            elif ymax <= 2.5:
                tick_size = 0.5
            else:
                tick_size = 1.0

            ymax = (int(ymax / tick_size) + 1) * tick_size
            ax.set_ylim([0, ymax])
            ax.yaxis.set_major_locator(plt.MultipleLocator(tick_size))
            if tick_size >= 1:
                ax.yaxis.set_major_formatter('  {x:.0f}')
            else:
                ax.yaxis.set_major_formatter('{x:.1f}')

            titles = {
                "seq_pread": "Sequential Read",
                "seq_pwrite": "Sequential Overwrite",
                "rnd_pread": "Random Read",
                "rnd_pwrite": "Random Overwrite",
                "append_pwrite": "Append",
                "cow": "Sub-Block Overwrite",
            }
            ax.set_title(titles.get(name), pad=3, fontsize=12)

        plot_single_bm(
            df,
            name=name,
            result_dir=result_dir,
            post_plot=post_plot,
            markers=("o", "^", "s", "D", "D"),
            colors=("tab:blue", "tab:orange", "tab:green", "tab:red", "tab:pink"),
        )


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-r", "--result_dir", help="Directory with results", type=Path,
                        default=get_latest_result(root_dir / "results" / "micro_st" / "exp"))
    args = parser.parse_args()
    plot_st(args.result_dir)
