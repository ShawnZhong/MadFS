#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot")


def read_files(result_dir, post_process_fn):
    data = pd.DataFrame()

    for path in Path(result_dir).glob("*"):
        if not path.is_dir():
            continue
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
        logx=False,
        logy=False,
        xlabel=None,
        ylabel=None,
        title=None,
        output_path=None,
):
    plt.clf()
    label_groups = df.groupby("label")
    for label, group in label_groups:
        if barchart:
            plt.bar(group["x"], group["y"], label=label)
        else:
            plt.plot(group["x"], group["y"], label=label, marker=".")

    if logx:
        plt.xscale("log")
    if logy:
        plt.yscale("log")

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    if len(label_groups) > 1:
        plt.legend()

    if output_path:
        plt.savefig(output_path, bbox_inches="tight")

    plt.show()


def plot_benchmarks(result_dir, data, **kwargs):
    for benchmark_name, benchmark in data.groupby("benchmark"):
        output_path = result_dir / f"{benchmark_name}.pdf"
        kwargs = {"ylabel": "Throughput (GB/s)", **kwargs}
        plot_single_bm(
            benchmark, title=benchmark_name, output_path=output_path, **kwargs,
        )


def parse_name(name, i):
    return re.split("[/:]", name)[i]


def format_1024(x):
    if int(x) % 1024 == 0:
        return f"{int(x) // 1024}K"
    return x


def plot_micro_st(result_dir):
    def post_process(df):
        df["benchmark"] = df["name"].apply(parse_name, args=(0,))
        df["x"] = df["name"].apply(parse_name, args=(1,)).apply(format_1024)
        df["y"] = df["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)

    data = read_files(result_dir, post_process)
    plot_benchmarks(result_dir, data, xlabel="Number of Bytes")


def plot_micro_mt(result_dir):
    def post_process(df):
        df["benchmark"] = df["name"].apply(parse_name, args=(0,))
        df["x"] = df["name"].apply(parse_name, args=(-1,)).apply(int)
        df["y"] = df["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)

    data = read_files(result_dir, post_process)
    plot_benchmarks(result_dir, data, xlabel="Number of Threads")


def plot_micro_meta(result_dir):
    def post_process(df):
        df["benchmark"] = df["name"].apply(parse_name, args=(0,))
        df["x"] = df["name"].apply(parse_name, args=(1,))
        df["y"] = df["cpu_time"].apply(lambda x: float(x) / 1000)

    data = read_files(result_dir, post_process)
    plot_benchmarks(
        result_dir,
        data,
        xlabel="Transaction History Length",
        ylabel="Latency (us)",
        logy=True,
    )


def plot_ycsb(result_dir):
    results = []
    for path in Path(result_dir).glob("*"):
        if not path.is_dir():
            continue
        fs_name = path.name

        for w in ("a", "b", "c", "d", "e", "f"):
            result_path = path / f"{w}-run.log"
            if not result_path.exists():
                logger.warning(f"{result_path} does not exist")
                continue
            with open(result_path, "r") as f:
                data = f.read()
                total_num_requests = sum(int(e) for e in re.findall('Finished (.+?) requests', data))
                total_time_us = sum(float(e) for e in re.findall('Time elapsed: (.+?) us', data))
                mops_per_sec = total_num_requests / total_time_us
                results.append({"benchmark": w, "x": fs_name, "y": mops_per_sec})
    df = pd.DataFrame(results)
    df["label"] = ""
    print(df)
    plot_benchmarks(
        result_dir, df, xlabel="Filesystem", ylabel="Throughput (Mops/s)", barchart=True
    )
