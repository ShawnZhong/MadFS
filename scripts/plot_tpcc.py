#!/usr/bin/env python3

import re
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

from plot_utils import export_results, plot_single_bm, get_sorted_subdirs, get_latest_result
from utils import root_dir

name_mapping = {
    "New\nOrder": "neword",
    "Payment": "payment",
    "Order\nStatus": "ordstat",
    "Delivery": "delivery",
    "Stock\nLevel": "slev",
}


def parse_results(result_dir):
    results = []
    for path in get_sorted_subdirs(result_dir):
        fs_name = path.name
        result_path = path / "start" / "prog.log"
        if not result_path.exists():
            print(f"{result_path} does not exist")
            continue
        with open(result_path, "r") as f:
            data = f.read()

            # result = {}
            total_tx = 0
            total_time_ms = 0
            for i, (name, workload) in enumerate(name_mapping.items()):
                num_tx = float(re.search(f"\[{i}\] sc:(.+?) lt:", data).group(1))
                time_ms = (
                    float(
                        re.search(
                            f"{workload}: timing = (.+?) nanoseconds", data
                        ).group(1)
                    )
                    / 1000 ** 2
                )
                results.append(
                    {
                        "x": name,
                        "y": num_tx / time_ms,
                        "label": fs_name,
                        "benchmark": "tpcc",
                    }
                )
                total_tx += num_tx
                total_time_ms += time_ms
            results.append(
                {
                    "x": "Mix",
                    "y": total_tx / total_time_ms,
                    "label": fs_name,
                    "benchmark": "tpcc",
                }
            )
    return pd.DataFrame(results)


def plot_tpcc(result_dir):
    df = parse_results(result_dir)
    export_results(result_dir, df)

    def post_plot(ax, **kwargs):
        _, ymax = ax.get_ylim()
        ymax = (int(ymax / 2) + 1) * 2
        tick_size = 2
        ax.set_ylim([0, ymax])
        ax.yaxis.set_major_locator(plt.MultipleLocator(tick_size))

        plt.xlabel("Transaction Type")
        plt.ylabel("Throughput (Kops/s)")
        plt.legend()

    plot_single_bm(
        df,
        barchart=True,
        figsize=(5, 2.5),
        result_dir=result_dir,
        post_plot=post_plot,
        separate_legend=False,
    )


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-r", "--result_dir", help="Directory with results", type=Path,
                        default=get_latest_result(root_dir / "results" / "bench_tpcc"))
    args = parser.parse_args()
    plot_tpcc(args.result_dir)
