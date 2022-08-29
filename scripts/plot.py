#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

from fs import all_fs

pd.options.display.max_rows = 100
pd.options.display.max_columns = 100
pd.options.display.width = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot")
plt.set_loglevel('WARNING')


def get_sorted_subdirs(path):
    order = {k: i for i, k in enumerate(all_fs)}

    paths = list(Path(path).glob("*"))
    paths = [p for p in paths if p.is_dir()]
    paths.sort(key=lambda x: order.get(x.name, 100))
    return paths


def get_fs_name(path):
    names = {
        "uLayFS": "MEFS",
    }
    return names.get(path.name, path.name)


def read_files(result_dir):
    if not result_dir.exists():
        raise FileNotFoundError(f"{result_dir} does not exist")

    data = pd.DataFrame()

    for path in get_sorted_subdirs(result_dir):
        fs_name = get_fs_name(path)
        result_path = path / "result.json"

        if not result_path.exists():
            logger.warning(f"{result_path} does not exist")
            continue

        with open(result_path, "r") as f:
            json_data = json.load(f)
            df = pd.DataFrame.from_dict(json_data["benchmarks"])
            df["label"] = fs_name
            data = pd.concat([data, df])

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


def save_fig(fig, name, result_dir):
    fig.savefig(result_dir / f"{name}.png", bbox_inches="tight", pad_inches=0, dpi=300)
    fig.savefig(result_dir / f"{name}.pdf", bbox_inches="tight", pad_inches=0)


def plot_single_bm(
        df,
        result_dir,
        barchart=False,
        name=None,
        post_plot=None,
        figsize=(2.5, 1.5),
        markers=("o", "^", "s", "D"),
        colors=(None,),
):
    plt.clf()
    fig, ax = plt.subplots(figsize=figsize)

    label_groups = df.groupby("label", sort=False)
    num_groups = len(label_groups)
    if len(markers) < num_groups:
        markers = markers * (num_groups // len(markers) + 1)
    if len(colors) < num_groups:
        colors = colors * (num_groups // len(colors) + 1)
    for (label, group), marker, color in zip(label_groups, markers, colors):
        if barchart:
            plt.bar(group["x"], group["y"], label=label, color=color)
        else:
            plt.plot(group["x"], group["y"], label=label, marker=marker, markersize=3, color=color)

    if post_plot:
        post_plot(ax=ax, name=name, df=df)

    save_fig(fig, name, result_dir)

    figlegend = plt.figure()
    figlegend.legend(
        *ax.get_legend_handles_labels(),
        ncol=4,
        loc="center",
        fontsize=8,
        columnspacing=2,
        handlelength=0,
        frameon=False,
        markerscale=1,
    )
    save_fig(figlegend, "legend", result_dir)


def parse_name(name, i):
    return re.split("[/:]", name)[i]


def plot_micro_st(result_dir):
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
            df["x"] = df["name"].apply(parse_name, args=(1,)).apply(lambda x: f"{int(x) / 1024:1g}")
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

            ax.yaxis.set_major_locator(plt.MaxNLocator(steps=[1, 5, 10]))
            ax.set_ylim(bottom=0)

            titles = {
                "seq_pread": "Sequential Read",
                "seq_pwrite": "Sequential Overwrite",
                "rnd_pread": "Random Read",
                "rnd_pwrite": "Random Overwrite",
                "append": "Append",
                "cow": "Sub-Block Overwrite",
            }
            ax.set_title(titles.get(name), pad=3, fontsize=12)

        plot_single_bm(
            df,
            name=name,
            result_dir=result_dir,
            post_plot=post_plot,
        )


def plot_micro_mt(result_dir):
    df = read_files(result_dir)
    df["benchmark"] = df["name"].apply(parse_name, args=(0,))
    df["x"] = df["name"].apply(parse_name, args=(-1,))
    xlabel = "Threads"

    benchmarks = df.groupby("benchmark")
    is_cc = "OCC" in df["label"].unique()

    for name, benchmark in benchmarks:
        benchmark["y"] = benchmark["bytes_per_second"].apply(lambda x: float(x) / 1024 ** 3)
        ylabel = "Throughput (GB/s)"

        export_results(result_dir, benchmark, name=name)

        def post_plot(ax, **kwargs):
            # if "tx_commit" in benchmark.columns:
            #     ax2 = ax.twinx()
            #     y = benchmark["tx_commit"] - 1
            #     y[y == 0] = None
            #     ax2.plot(benchmark["x"], y, ":", label="tx_commit")
            #     ax2.set_ylim(bottom=0)
            #     ax2.yaxis.set_major_locator(plt.MaxNLocator(steps=[1, 5, 10]))
            #     ax2.set_ylabel("uLayFS commit conflicts per Tx")

            ax.set_xlabel(xlabel, labelpad=0)
            ax.set_ylabel(ylabel, labelpad=0)

            labels = benchmark["x"].unique()
            plt.xticks(ticks=labels, labels=labels)
            ax.xaxis.set_major_locator(plt.MaxNLocator(6))
            ax.yaxis.set_major_locator(plt.MultipleLocator(1))
            ax.set_ylim(bottom=0)

            titles = {
                "unif_0R": "100% Write",
                "unif_50R": "50% Read + 50% Write",
                "unif_95R": "95% Read + 5% Write",
                "unif_100R": "100% Read",
                "zipf_4k": r"4 KB Write w/ Zipf",
                "zipf_2k": r"2 KB Write w/ Zipf",
            }

            ax.set_title(titles.get(name), pad=3, fontsize=11)

        if is_cc:
            plot_single_bm(
                benchmark,
                name=name,
                result_dir=result_dir,
                post_plot=post_plot,
                markers=("o",),
                colors=("tab:blue", "tab:cyan", "tab:purple", "tab:pink"),
            )
        else:
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
