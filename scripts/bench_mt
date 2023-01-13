#!/usr/bin/env python3
import argparse
import logging

from args import add_common_args, parse_args, add_gbench_args, process_gbench_args
from bench_utils import run_gbench
from plot_mt import plot_mt
from utils import root_dir, get_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench_mt")


def main(**kwargs):
    result_dir = root_dir / "results" / "micro_mt" / "exp" / get_timestamp()
    run_gbench(cmake_target="micro_mt", result_dir=result_dir, **kwargs)
    plot_mt(result_dir)
    logger.info(f"Results saved to {result_dir}")


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    add_common_args(argparser)
    add_gbench_args(argparser)

    args, run_cfg = parse_args(argparser)
    process_gbench_args(args, run_cfg)
    logger.info(f"args={args}, run_config={run_cfg}")
    main(**vars(args), run_config=run_cfg)
