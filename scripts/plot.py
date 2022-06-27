#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path

import matplotlib
import pandas as pd
from matplotlib import pyplot as plt

pd.options.display.max_rows = 100
pd.options.display.max_columns = 100
pd.options.display.width = None

matplotlib.rcParams["legend.columnspacing"] = 0.5
matplotlib.rcParams["legend.fontsize"] = 6

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot")
plt.set_loglevel('WARNING')


def get_sorted_subdirs(path):
    order = {
        "uLayFS": 1,
        "ext4": 2,
        "ext4-DAX": 2,
        "NOVA": 3,
    }

    paths = list(Path(path).glob("*"))
    paths = [p for p in paths if p.is_dir()]
    paths.sort(key=lambda x: order.get(x.name, 100))
    return paths


def read_files(result_dir):
    data = pd.DataFrame()

    for path in get_sorted_subdirs(result_dir):
        fs_name = path.name
        result_path = path / "result.json"

        if not result_path.exists():
            logger.warning(f"{result_path} does not exist")
            continue

        with open(result_path, "r") as f:
            json_data = json.load(f)
            df = pd.DataFrame.from_dict(json_data["benchmarks"])
            df["label"] = fs_name
            data = data.append(df)

    return data


def export_results(result_dir, data, name="result"):
    with open(result_dir / f"{name}.csv", "w") as f:
        data.to_csv(f)
    with open(result_dir / f"{name}.txt", "w") as f:
        for name, benchmark in data[["benchmark", "label", "x", "y"]].groupby(["benchmark"], sort=False):
            pt = pd.pivot_table(benchmark, values="y", index="x", columns="label", sort=False)
            if "uLayFS" in pt.columns:
                for c in pt.columns:
                    pt[f"{c}%"] = pt[c] / pt["uLayFS"] * 100
            print(name)
            print(pt)
            print(name, file=f)
            print(pt, file=f)


def plot_single_bm(
        df,
        result_dir,
        barchart=False,
        name=None,
        post_plot=None,
        figsize=(2.75, 2.75),
):
    plt.clf()
    fig, ax = plt.subplots(figsize=figsize)
    label_groups = df.groupby("label", sort=False)
    for label, group in label_groups:
        if barchart:
            plt.bar(group["x"], group["y"], label=label)
        else:
            plt.plot(group["x"], group["y"], label=label, marker=".")

    if post_plot:
        post_plot(ax=ax, name=name, df=df)

    plt.savefig(result_dir / f"{name}.png", bbox_inches="tight", dpi=300)
    plt.savefig(result_dir / f"{name}.pdf", bbox_inches="tight")


def parse_name(name, i):
    return re.split("[/:]", name)[i]


def format_bytes(x):
    if int(x) % 1024 == 0:
        return f"{int(x) // 1024}K"
    return str(x)


def plot_micro_st(result_dir):
    df = read_files(result_dir)
    df["benchmark"] = df["name"].apply(parse_name, args=(0,))
    xlabel = "I/O Size (Bytes)"

    for name, benchmark in df.groupby("benchmark"):
        if name.startswith("cow"):
            df["x"] = df["name"].apply(parse_name, args=(1,))
            df["y"] = df["real_time"].apply(lambda x: float(x) / 1000)
            ylabel = "Latency (ms)"
        else:
            df["x"] = df["name"].apply(parse_name, args=(1,)).apply(format_bytes)
            df["y"] = df["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)
            ylabel = "Throughput (GB/sec)"

        export_results(result_dir, benchmark, name=name)

        def post_plot(ax, **kwargs):
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)

            plt.xticks(rotation=45)
            ax.xaxis.set_major_locator(plt.MultipleLocator(4))

            ax.yaxis.set_major_locator(plt.MaxNLocator(steps=[1, 5, 10]))
            ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.1f'))
            ax.set_ylim(bottom=0)

            ax.legend(ncol=2)
            plt.title(name)

        plot_single_bm(
            benchmark,
            name=name,
            result_dir=result_dir,
            post_plot=post_plot,
        )


def plot_micro_mt(result_dir):
    df = read_files(result_dir)
    df["benchmark"] = df["name"].apply(parse_name, args=(0,))
    df["x"] = df["name"].apply(parse_name, args=(-1,))
    xlabel = "Number of Threads"

    for name, benchmark in df.groupby("benchmark"):
        if name.startswith("no_overlap"):
            benchmark["y"] = benchmark["items_per_second"].apply(lambda x: float(x) / 1000 ** 2)
            ylabel = "Throughput (Mops/sec)"
        else:
            benchmark["y"] = benchmark["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)
            ylabel = "Throughput (GB/sec)"

        export_results(result_dir, benchmark, name=name)

        def post_plot(ax, **kwargs):
            if name.startswith("zipf") and "uLayFS" in df["label"].unique():
                ax2 = ax.twinx()
                ax2.plot(df["x"], df["tx_commit"] - 1, ":", label="tx_commit")
                ax2.set_ylabel("uLayFS commit conflicts per Tx")

            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)

            labels = benchmark["x"].unique()
            plt.xticks(ticks=labels, labels=labels)
            ax.xaxis.set_major_locator(plt.MultipleLocator(3))

            ax.set_ylim(bottom=0)

            ax.legend(ncol=2, loc="lower left")
            plt.title(name)

        plot_single_bm(
            benchmark,
            name=name,
            result_dir=result_dir,
            post_plot=post_plot,
        )


def plot_micro_meta(result_dir):
    df = read_files(result_dir)
    df["benchmark"] = df["name"].apply(parse_name, args=(0,))
    df["x"] = df["name"].apply(parse_name, args=(1,))
    df["y"] = df["cpu_time"].apply(lambda x: float(x) / 1000)

    export_results(result_dir, df)

    def post_plot(ax, **kwargs):
        plt.xlabel("Transaction History Length")
        plt.ylabel("Latency (us)")

    for name, benchmark in df.groupby("benchmark"):
        plot_single_bm(
            benchmark,
            name=name,
            result_dir=result_dir,
            post_plot=post_plot,
        )


def plot_ycsb(result_dir):
    results = []
    for path in get_sorted_subdirs(result_dir):
        fs_name = path.name

        for w in ("a", "b", "c", "d", "e", "f"):
            result_path = path / f"{w}-run.log"
            if not result_path.exists():
                logger.warning(f"{result_path} does not exist")
                continue
            with open(result_path, "r") as f:
                data = f.read()
                total_num_requests = sum(
                    int(e) for e in re.findall("Finished (.+?) requests", data)
                )
                total_time_us = sum(
                    float(e) for e in re.findall("Time elapsed: (.+?) us", data)
                )
                mops_per_sec = total_num_requests / total_time_us
                results.append({"x": w, "y": mops_per_sec, "label": fs_name})
    df = pd.DataFrame(results)
    df_pivot = pd.pivot_table(df, values="y", index="x", columns="label", sort=False)
    df_pivot = df_pivot[df["label"].unique()]
    df_pivot.plot(
        kind="bar",
        figsize=(5, 2.5),
        rot=0,
        legend=False,
        ylabel="Throughput (Mops/s)",
        xlabel="Workload",
    )
    plt.legend()
    plt.savefig(result_dir / "ycsb.pdf", bbox_inches="tight")
    if "uLayFS" in df_pivot.columns:
        for c in df_pivot.columns:
            df_pivot[f"{c}%"] = df_pivot["uLayFS"] / df_pivot[c] * 100
    print(df_pivot)
    with open(result_dir / "ycsb.txt", "w") as f:
        print(df_pivot, file=f)


def plot_tpcc(result_dir):
    name_mapping = {
        "New\nOrder": "neword",
        "Payment": "payment",
        "Order\nStatus": "ordstat",
        "Delivery": "delivery",
        "Stock\nLevel": "slev"
    }

    results = []
    for path in get_sorted_subdirs(result_dir):
        fs_name = path.name
        result_path = path / "start" / "prog.log"
        if not result_path.exists():
            logger.warning(f"{result_path} does not exist")
            continue
        with open(result_path, "r") as f:
            data = f.read()

            result = {}
            total_tx = 0
            total_time_ms = 0
            for i, (name, workload) in enumerate(name_mapping.items()):
                num_tx = float(re.search(f"\[{i}\] sc:(.+?) lt:", data).group(1))
                time_ms = float(re.search(f"{workload}: timing = (.+?) nanoseconds", data).group(1)) / 1000 ** 2
                result[name] = num_tx / time_ms  # kops/s
                total_tx += num_tx
                total_time_ms += time_ms
            result["Mix"] = total_tx / total_time_ms  # kops/s
            result["label"] = fs_name
            results.append(result)
    df = pd.DataFrame(results)
    df.set_index("label", inplace=True)
    df = df.T
    df.plot(
        kind="bar",
        rot=0,
        figsize=(5, 2.5),
        legend=False,
        ylabel="Throughput (k txns/s)",
        xlabel="Transaction Type",
    )
    plt.legend()
    plt.savefig(result_dir / "tpcc.pdf", bbox_inches="tight")

    for c in df.columns:
        df[f"{c}%"] = df["uLayFS"] / df[c] * 100
    print(df)
    with open(result_dir / "tpcc.txt", "w") as f:
        print(df, file=f)
