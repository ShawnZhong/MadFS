import argparse


def add_common_args(argparser: argparse.ArgumentParser):
    from fs import bench_fs, available_fs
    from runner import build_types

    argparser.add_argument(
        "-b",
        "--build_type",
        choices=build_types,
    )
    argparser.add_argument(
        "-c",
        "--cmake_args",
        default="",
        help="additional build arguments to be passed to CMake",
    )
    argparser.add_argument(
        "-f",
        "--fs_names",
        default=bench_fs.keys(),
        choices=available_fs.keys(),
        nargs="+",
        help="Filesystems to run",
    )

    # args to be parsed to run_config
    argparser.add_argument(
        "prog_args",
        nargs="*",
        help="additional arguments to be passed to the program during execution",
    )
    argparser.add_argument(
        "--trace",
        action="store_true",
        help="Run with tracing enabled",
    )

    # add benchmark specific arguments to run_config
    argparser.add_argument(
        "--filter",
        help="filters to be passed to Google Benchmark"
    )
    argparser.add_argument(
        "--size",
        help="Filesize in MB used for benchmarks",
    )
    argparser.add_argument(
        "--iter",
        help="Number of iterations for benchmarks",
    )
    argparser.add_argument(
        "--repeat",
        help="Number of repeats for benchmarks",
    )


def parse_args(argparser: argparse.ArgumentParser):
    cmd_args = argparser.parse_intermixed_args()

    run_config = {
        "trace": cmd_args.__dict__.pop("trace"),
        "prog_args": cmd_args.__dict__.pop("prog_args"),
        "env": {},
    }

    bm_filter = cmd_args.__dict__.pop("filter")
    if bm_filter:
        run_config["prog_args"] += [f"--benchmark_filter={bm_filter}"]

    bm_repeat = cmd_args.__dict__.pop("repeat")
    if bm_repeat:
        run_config["prog_args"] += [f"--benchmark_repetitions={bm_repeat}"]

    bm_size = cmd_args.__dict__.pop("size")
    if bm_size:
        run_config["env"]["BENCH_FILE_SIZE"] = bm_size

    bm_iter = cmd_args.__dict__.pop("iter")
    if bm_iter:
        run_config["env"]["BENCH_NUM_ITER"] = bm_iter

    return cmd_args, run_config
