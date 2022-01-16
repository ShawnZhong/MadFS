#!/usr/bin/env python3

import json
import logging
import os
import re
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot")
plt.set_loglevel('WARNING')


def read_files(result_dir, post_process_fn):
    data = pd.DataFrame()

    paths = list(Path(result_dir).glob("*"))
    paths = [p for p in paths if p.is_dir()]
    paths.sort(key=os.path.getmtime)
    for path in paths:
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

    if len(label_groups) > 1:
        plt.legend()

    if output_path:
        plt.savefig(output_path, bbox_inches="tight")

    plt.show()


def plot_benchmarks(result_dir, data, **kwargs):
    for name, benchmark in data.groupby("benchmark"):
        output_path = result_dir / f"{name}.pdf"
        plot_single_bm(
            benchmark, name=name, output_path=output_path, **kwargs,
        )


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

    def post_plot(**kwargs):
        plt.xticks(rotation=45)

    data = read_files(result_dir, post_process)
    plot_benchmarks(result_dir, data, post_plot=post_plot, xlabel="Number of Bytes", )


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
        ax.yaxis.set_major_locator(plt.MaxNLocator(steps=[1, 10]))
        ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.1f'))
        if name == "srmw":
            plt.xlabel("Number of Writer Threads")
        else:
            plt.xlabel("Number of Threads")

    data = read_files(result_dir, post_process)
    plot_benchmarks(result_dir, data, post_plot=post_plot)


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
    )


def plot_ycsb(result_dir):
    results = []
    paths = list(Path(result_dir).glob("*"))
    paths = [p for p in paths if p.is_dir()]
    paths.sort(key=os.path.getmtime)

    for path in paths:
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
    print(df_pivot)
    df_pivot.plot(
        kind="bar",
        figsize=(5, 4),
        legend=False,
        ylabel="Throughput (Mops/s)",
        xlabel="Workload",
    )
    plt.legend()
    plt.savefig(result_dir / "ycsb.pdf", bbox_inches="tight")


def plot_tpcc(result_dir):
    results = []
    paths = list(Path(result_dir).glob("*"))
    paths = [p for p in paths if p.is_dir()]
    paths.sort(key=os.path.getmtime)

    for path in paths:
        fs_name = path.name
        result_path = path / "start" / "prog.log"
        if not result_path.exists():
            logger.warning(f"{result_path} does not exist")
            continue
        with open(result_path, "r") as f:
            data = f.read()
            result = {
                k: int(re.search(f"{k}: timing = (.+?) nanoseconds", data).group(1)) / 1000 ** 3
                for k in ("neword", "payment", "ordstat", "delivery", "slev")
            }
            result["label"] = fs_name
            results.append(result)
    df = pd.DataFrame(results)
    print(df)
