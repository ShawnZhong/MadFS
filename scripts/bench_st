#!/usr/bin/env python3
import argparse
import logging

from args import add_common_args, parse_args, process_gbench_args, add_gbench_args
from bench_utils import run_gbench
from plot_st import plot_st
from utils import get_timestamp, root_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench_st")


def main(**kwargs):
    result_dir = root_dir / "results" / "micro_st" / "exp" / get_timestamp()
    run_gbench(cmake_target="micro_st", result_dir=result_dir, **kwargs)
    plot_st(result_dir)
    logger.info(f"Results saved to {result_dir}")


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    add_common_args(argparser)
    add_gbench_args(argparser)

    args, run_cfg = parse_args(argparser)
    process_gbench_args(args, run_cfg)
    logger.info(f"args={args}, run_config={run_cfg}")
    main(**vars(args), run_config=run_cfg)
