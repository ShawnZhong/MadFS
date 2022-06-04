#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

pd.options.display.max_rows = 100
pd.options.display.max_columns = 100

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


def read_files(result_dir, post_process_fn):
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
            post_process_fn(df)
            data = data.append(df)

    return data


def plot_single_bm(
        df,
        barchart=False,
        xlabel=None,
        ylabel=None,
        title=None,
        name=None,
        output_path=None,
        post_plot=None,
        figsize=(3, 3),
):
    plt.clf()
    fig, ax = plt.subplots(figsize=figsize)
    label_groups = df.groupby("label", sort=False)
    for label, group in label_groups:
        if barchart:
            plt.bar(group["x"], group["y"], label=label)
        else:
            plt.plot(group["x"], group["y"], label=label, marker=".")

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    if post_plot:
        post_plot(ax=ax, name=name, df=df)

    if output_path:
        plt.savefig(output_path, bbox_inches="tight")


def plot_benchmarks(result_dir, data, **kwargs):
    for name, benchmark in data.groupby("benchmark"):
        output_path = result_dir / f"{name}.pdf"
        plot_single_bm(
            benchmark, name=name, output_path=output_path, **kwargs,
        )


def export_results(result_dir, data):
    with open(result_dir / f"result.csv", "w") as f:
        data.to_csv(f)
    with open(result_dir / f"result.txt", "w") as f:
        for name, benchmark in data[["benchmark", "label", "x", "y"]].groupby(["benchmark"], sort=False):
            pt = pd.pivot_table(benchmark, values="y", index="x", columns="label", sort=False)
            if "uLayFS" in pt.columns:
                for c in pt.columns:
                    pt[f"{c}%"] = pt["uLayFS"] / pt[c] * 100
            print(name)
            print(pt)
            print(name, file=f)
            print(pt, file=f)


def parse_name(name, i):
    return re.split("[/:]", name)[i]


def format_bytes(x):
    if int(x) % 1024 == 0:
        return f"{int(x) // 1024}K"
    return str(x)


def plot_micro_st(result_dir):
    def post_process(df):
        df["benchmark"] = df["name"].apply(parse_name, args=(0,))
        df["x"] = df["name"].apply(parse_name, args=(1,)).apply(format_bytes)
        df["y"] = df["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)

    def post_plot(name, ax, **kwargs):
        plt.xticks(rotation=45)
        ax.yaxis.set_major_locator(plt.MaxNLocator(steps=[1, 5, 10]))
        ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.1f'))
        ax.set_ylim(bottom=0)
        plt.legend()

    data = read_files(result_dir, post_process)
    export_results(result_dir, data)
    plot_benchmarks(
        result_dir,
        data,
        post_plot=post_plot,
        figsize=(2.75, 2.75),
        xlabel="I/O Size (Bytes)",
        ylabel="Throughput (GB/s)",
    )


def plot_micro_mt(result_dir):
    def post_process(df):
        df["benchmark"] = df["name"].apply(parse_name, args=(0,))
        df["x"] = df["name"].apply(parse_name, args=(-1,))
        df["y"] = df["items_per_second"].apply(lambda x: float(x) / 1000 ** 2)

        srmw_filter = df["benchmark"] == "srmw"
        df.loc[srmw_filter, "x"] = df.loc[srmw_filter, "x"].apply(lambda x: str(int(x) - 1))
        df.drop(df[df["x"] == "0"].index, inplace=True)

    def post_plot(name, df, ax, **kwargs):
        labels = df["x"].unique()
        plt.xticks(ticks=labels, labels=labels)
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_locator(plt.MaxNLocator(steps=[1, 2]))
        plt.legend()

    data = read_files(result_dir, post_process)
    export_results(result_dir, data)
    plot_benchmarks(
        result_dir,
        data,
        post_plot=post_plot,
        xlabel="Number of Threads",
        ylabel="Throughput (Mops/sec)",
    )


def plot_micro_meta(result_dir):
    def post_process(df):
        df["benchmark"] = df["name"].apply(parse_name, args=(0,))
        df["x"] = df["name"].apply(parse_name, args=(1,))
        df["y"] = df["cpu_time"].apply(lambda x: float(x) / 1000)

    data = read_files(result_dir, post_process)
    export_results(result_dir, data)
    plot_benchmarks(
        result_dir,
        data,
        xlabel="Transaction History Length",
        ylabel="Latency (us)",
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
