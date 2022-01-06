import logging
import pprint
from pathlib import Path

from utils import get_timestamp, system, chdir_to_root

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("runner")


class Runner:
    def __init__(self, cmake_target, build_type=None, output_path=None, **kwargs):
        chdir_to_root()

        self.is_micro = cmake_target == "micro"
        self.is_bench = self.is_micro or cmake_target.startswith("leveldb")

        if build_type is None:
            build_type = "release" if self.is_bench else "debug"
        if output_path is None:
            output_path = Path("results") / cmake_target / build_type / get_timestamp()

        self.build_type = build_type
        self.build_path = Path(f"build-{build_type}")
        self.cmake_target = cmake_target
        self.output_path = output_path
        self.exe_path = None
        self.ulayfs_path = None

        self.output_path.mkdir(parents=True, exist_ok=True)

    def build(self, cmake_args="", link_ulayfs=True, **kwargs):
        if self.is_bench:
            cmake_args += " -DULAYFS_BUILD_BENCH=ON "

        config_log_path = self.output_path / "config.log"
        build_log_path = self.output_path / "build.log"

        cmake_args += f" -DULAYFS_LINK_LIBRARY={'ON' if link_ulayfs else 'OFF'} "

        # build
        system(
            f"make {self.build_type} "
            f"CMAKE_ARGS='{cmake_args}' "
            f"BUILD_TARGETS='{self.cmake_target} ulayfs' ",
            log_path=build_log_path,
        )

        # save config
        with open(config_log_path, "w") as fout:
            pprint.pprint(locals(), stream=fout)
        system(f"cmake -L -N {self.build_path} >> {config_log_path}")

        self.ulayfs_path = self.build_path / "libulayfs.so"
        self.exe_path = self.build_path / self.cmake_target

    def run(self, prog_args="", load_so=False, **kwargs):
        assert self.exe_path is not None

        cmd = f"{self.exe_path} {prog_args}"
        run_log_path = self.output_path / "run.log"

        if load_so:
            assert self.ulayfs_path is not None
            cmd = f"env LD_PRELOAD={self.ulayfs_path} {cmd}"
        if self.is_micro:
            prog_args += " --benchmark_counters_tabular=true "

        # execute
        if self.build_type == "pmemcheck":
            pmemcheck_dir = self.build_path / "_deps" / "pmemcheck-src"
            system(
                f"VALGRIND_LIB={pmemcheck_dir}/libexec/valgrind/ "
                f"{pmemcheck_dir}/bin/valgrind --tool=pmemcheck --trace-children=yes "
                f"{cmd}",
                log_path=run_log_path,
            )

        elif self.build_type == "profile":
            perf_data = self.output_path / "perf.data"
            flamegraph_output = self.output_path / "flamegraph.svg"
            flamegraph_dir = self.build_path / "_deps" / "flamegraph-src"

            # record perf data
            system(
                f"perf record --freq=997 --call-graph dwarf -o {perf_data} {cmd}",
                log_path=run_log_path,
            )

            # show perf results in terminal
            system(f"perf report -i {perf_data}")

            # generate flamegraph
            system(
                f"perf script -i {perf_data} | "
                f"{flamegraph_dir}/stackcollapse-perf.pl | "
                f"{flamegraph_dir}/flamegraph.pl > {flamegraph_output}"
            )
            logger.info(f"The flamegraph is available at `{flamegraph_output}`")

        else:
            system(cmd, log_path=run_log_path)
