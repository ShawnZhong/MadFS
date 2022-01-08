#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot")


def read_file(filepath):
    def parse(name, i):
        return re.split("[/:]", name)[i]

    def format_x(x):
        if int(x) % 1024 == 0:
            return f"{int(x) // 1024}K"
        return x

    with open(filepath, "r") as f:
        json_data = json.load(f)
        data = pd.DataFrame.from_dict(json_data["benchmarks"])
        data["benchmark"] = data["name"].apply(parse, args=(0,))
        data["x"] = data["name"].apply(parse, args=(1,)).apply(format_x)
        data["y"] = data["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)
        return data


def read_files(result_dir):
    data = pd.DataFrame()

    for path in Path(result_dir).glob('*'):
        if not path.is_dir():
            continue
        fs_name = path.name
        result_path = path / 'result.json'

        if not result_path.exists():
            logger.warning(f"{result_path} does not exist")
            continue

        res = read_file(result_path)
        res["label"] = fs_name
        data = data.append(res)

    return data


def plot_single_bm(
        label_groups,
        logx=False,
        logy=False,
        xlabel=None,
        ylabel=None,
        title=None,
        output_path=None,
):
    plt.clf()
    for label, group in label_groups:
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


def plot_micro_st(result_dir):
    data = read_files(result_dir)
    for benchmark_name, benchmark in data.groupby("benchmark"):
        output_path = result_dir / f"{benchmark_name}.pdf"
        plot_single_bm(
            benchmark.groupby("label"),
            title=benchmark_name,
            xlabel="Number of Bytes",
            ylabel="Throughput (GB/s)",
            output_path=output_path,
        )


def plot_micro_mt(result_dir):
    data = read_files(result_dir)
    for benchmark_name, benchmark in data.groupby("benchmark"):
        output_path = result_dir / f"{benchmark_name}.pdf"
        plot_single_bm(
            benchmark.groupby("label"),
            title=benchmark_name,
            xlabel="Number of Bytes",
            ylabel="Throughput (GB/s)",
            output_path=output_path,
        )
