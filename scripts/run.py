#!/usr/bin/env python3

import argparse
import logging

from args import add_common_args, parse_args, add_gbench_args, process_gbench_args
from fs import available_fs
from runner import Runner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build and run tests")
    parser.add_argument(
        "cmake_target",
        help="see .cpp files in `test` and `bench` for available targets",
    )
    add_common_args(parser)
    add_gbench_args(parser)
    # set the default argument of fs to MadFS
    parser.set_defaults(fs_names=["MadFS"])
    args, run_config = parse_args(parser)
    process_gbench_args(args, run_config)

    logger.info(f"args={args}, run_config={run_config}")

    for fs_name in args.fs_names:
        fs = available_fs[fs_name]
        runner = Runner(name=args.cmake_target, build_type=args.build_type)
        runner.build(cmake_args=args.cmake_args)
        runner.run(
            fs=fs,
            **run_config
        )
