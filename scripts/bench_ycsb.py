#!/usr/bin/env python3
import argparse
import itertools
import logging

from args import add_common_args, parse_args
from bench_utils import drop_cache
from fs import available_fs
from init import init
from plot_ycsb import plot_ycsb
from runner import Runner
from utils import root_dir, system, get_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench")


def prepare_files(size, workloads):
    data_dir = root_dir / "data" / "ycsb"
    ycsb_folder = data_dir / "ycsb-0.17.0"

    if not ycsb_folder.exists():
        url = "https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz"
        system(f"wget {url} -P {data_dir} && tar -xzf {data_dir / 'ycsb-0.17.0.tar.gz'} -C {data_dir}")

    for (w, t) in itertools.product(workloads, ("load", "run")):
        file = data_dir / f"{w}-{t}-{size}m.txt"
        if not file.exists():
            system(
                f"{ycsb_folder / 'bin' / 'ycsb.sh'} {t} basic "
                f"-P {ycsb_folder / 'workloads' / f'workload{w}'} "
                f"-p fieldcount=1 -p fieldlength=0 "
                f"-p recordcount={size}000000 -p operationcount={size}000000 "
                f"> {file}"
            )

    return data_dir


def bench_ycsb(size, result_dir, build_type, cmake_args, fs_names, run_config):
    init()
    workloads = ("a", "b", "c", "d", "e", "f")
    data_dir = prepare_files(size, workloads)

    for fs_name in fs_names:
        fs = available_fs[fs_name]
        dbdir = fs.path / "bench-dbdir"

        runner = Runner("leveldb_ycsb", result_dir=result_dir / fs_name, build_type=build_type)
        runner.build(cmake_args=cmake_args)

        for w in workloads:
            system(f"rm -rf {dbdir} && mkdir -p {dbdir}")
            for t in ("load", "run"):
                drop_cache()
                trace_name = f"{w}-{t}"
                trace_path = data_dir / f"{trace_name}-{size}m.txt"
                cmd = f"{runner.prog_path} -f {trace_path} -d {dbdir}".split()
                runner.run(cmd=cmd, fs=fs, prog_log_name=f"{trace_name}.log", **run_config)
            system(f"rm -rf {dbdir}")


def main(**kwargs):
    result_dir = root_dir / "results" / "leveldb_ycsb" / "exp" / get_timestamp()
    bench_ycsb(result_dir=result_dir, **kwargs)
    plot_ycsb(result_dir)
    logger.info(f"Results saved to {result_dir}")


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    add_common_args(argparser)

    argparser.add_argument("-s", "--size", type=int, default=1,
                           help="Size of the dataset in millions")

    args, run_cfg = parse_args(argparser)
    logger.info(f"args={args}, run_config={run_cfg}")
    main(**vars(args), run_config=run_cfg)
