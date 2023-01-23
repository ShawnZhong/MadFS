#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

pd.options.display.max_rows = 100
pd.options.display.max_columns = 100
pd.options.display.width = None

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot")
plt.set_loglevel("WARNING")
logging.getLogger("fontTools.subset").setLevel(logging.WARNING)


def get_sorted_subdirs(path):
    weights = {
        "MadFS": 1,
        "ext4-DAX": 2,
        "NOVA": 3,
        "SplitFS": 4,
        "OCC": 10,
        "Spinlock": 20,
        "Mutex": 30,
        "Rwlock": 40,
    }

    paths = list(Path(path).glob("*"))
    paths = [p for p in paths if p.is_dir()]
    paths.sort(key=lambda x: weights[x.name])
    return paths


def read_files(result_dir):
    if not result_dir.exists():
        raise FileNotFoundError(f"{result_dir} does not exist")

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
            data = pd.concat([data, df])

    return data


def export_results(result_dir, data, name="result"):
    with open(result_dir / f"{name}.csv", "w") as f:
        data.to_csv(f)
    with open(result_dir / f"{name}.txt", "w") as f:
        for name, benchmark in data[["benchmark", "label", "x", "y"]].groupby(
            ["benchmark"], sort=False
        ):
            pt = pd.pivot_table(
                benchmark, values="y", index="x", columns="label", sort=False
            )
            if "MadFS" in pt.columns:
                for c in pt.columns:
                    pt[f"{c}%"] = pt[c] / pt["MadFS"] * 100
            print(name)
            print(pt)
            print(name, file=f)
            print(pt, file=f)
    logger.info(f"Results saved to {result_dir}/{name}.txt")


def export_df(result_dir, df, name="result"):
    with open(result_dir / f"{name}.csv", "w") as f:
        df.to_csv(f)
    with open(result_dir / f"{name}.txt", "w") as f:
        df.to_string(f)
    logger.info(f"Results saved to {result_dir}/{name}.txt")


def save_fig(fig, name, result_dir):
    fig.savefig(result_dir / f"{name}.png", bbox_inches="tight", pad_inches=0, dpi=300)
    fig.savefig(result_dir / f"{name}.pdf", bbox_inches="tight", pad_inches=0)
    logger.info(f"Figure saved to {result_dir}/{name}.png")


def plot_single_bm(
    df,
    result_dir,
    barchart=False,
    name="result",
    post_plot=None,
    figsize=(2.5, 1.5),
    markers=("o", "^", "s", "D"),
    hatches=("//", "\\\\", "--", ".."),
    colors=("C3", "C0", "C2", "C1"),
    separate_legend=True,
):
    plt.clf()
    fig, ax = plt.subplots(figsize=figsize)

    label_groups = df.groupby("label", sort=False)
    num_groups = len(label_groups)
    if len(markers) < num_groups:
        markers = markers * (num_groups // len(markers) + 1)
    if len(colors) < num_groups:
        colors = colors * (num_groups // len(colors) + 1)
    if len(hatches) < num_groups:
        hatches = hatches * (num_groups // len(hatches) + 1)

    if barchart:
        x = np.arange(len(df["x"].unique()))
        width = 0.8 / num_groups
        offsets = np.linspace(-0.3, 0.3, num_groups)
        for (label, group), color, hatch, i in zip(
            label_groups, colors, hatches, range(num_groups)
        ):
            ax.bar(
                x + offsets[i],
                group["y"],
                width,
                label=label,
                color=color,
                hatch=hatch,
                alpha=1,
            )
        ax.set_xticks(x)
        ax.set_xticklabels(df["x"].unique())
    else:
        zorder = len(label_groups)
        for (label, group), marker, color in zip(label_groups, markers, colors):
            plt.plot(
                group["x"],
                group["y"],
                label=label,
                marker=marker,
                markersize=3,
                color=color,
                zorder=zorder,
            )
            zorder -= 1

    if post_plot:
        post_plot(ax=ax, name=name, df=df)

    save_fig(fig, name, result_dir)

    if separate_legend:
        figlegend = plt.figure()
        figlegend.legend(
            *ax.get_legend_handles_labels(),
            ncol=num_groups,
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
