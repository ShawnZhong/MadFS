import logging
import pprint

from fs import process_cmd
from utils import get_timestamp, system, root_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("runner")

build_types = [
    "debug",
    "release",
    "relwithdebinfo",
    "profile",
    "pmemcheck",
    "asan",
    "ubsan",
    "msan",
    "tsan",
]


class Runner:
    def __init__(self, name, build_type="release", result_dir=None):
        self.is_micro = name.startswith("micro")
        self.is_bench = any(name.startswith(x) for x in ["micro", "leveldb", "tpcc"])

        if result_dir is None:
            result_dir = root_dir / "results" / name / build_type / get_timestamp()

        self.name = name
        self.build_type = build_type
        self.build_path = root_dir / f"build-{build_type}"
        self.result_dir = result_dir
        self.prog_path = None

        self.result_dir.mkdir(parents=True, exist_ok=True)

    def build(self, cmake_target=None, cmake_args=""):
        if cmake_target is None:
            cmake_target = self.name
        if self.is_bench:
            cmake_args += " -DULAYFS_BUILD_BENCH=ON "

        # build
        build_log_path = self.result_dir / "build.log"
        system(
            f"make {self.build_type} -C {root_dir} "
            f"CMAKE_ARGS='{cmake_args}' "
            f"BUILD_TARGETS='{cmake_target} ulayfs' ",
            log_path=build_log_path,
        )

        # save config
        config_log_path = self.result_dir / "config.log"
        with open(config_log_path, "w") as fout:
            pprint.pprint(locals(), stream=fout)
        system(f"cmake -LA -N {self.build_path} >> {config_log_path}")

        self.prog_path = self.build_path / cmake_target

    def run(
            self,
            cmd=None,
            additional_args=None,
            prog_log_name="prog.log",
            fs="uLayFS",
            trace=False,
    ):
        if cmd is None:
            assert self.prog_path is not None
            cmd = f"{self.prog_path} "

        if self.is_micro:
            json_path = self.result_dir / "result.json"
            cmd += f" --benchmark_counters_tabular=true "
            cmd += f" --benchmark_out={json_path} "

        cmd = f"{cmd} {' '.join(additional_args)}" if additional_args else cmd
        cmd = process_cmd(fs, cmd, self.build_type)

        # execute
        prog_log_path = self.result_dir / prog_log_name
        if self.build_type == "pmemcheck":
            self._run_pmemcheck(cmd, log_path=prog_log_path)
        elif self.build_type == "profile":
            self._run_profile(cmd, log_path=prog_log_path)
        elif trace:
            self._run_trace(cmd, log_path=prog_log_path)
        else:
            system(cmd, log_path=prog_log_path)

    def _run_pmemcheck(self, cmd, log_path):
        pmemcheck_dir = self.build_path / "_deps" / "pmemcheck-src"
        system(
            f"VALGRIND_LIB={pmemcheck_dir}/libexec/valgrind/ "
            f"{pmemcheck_dir}/bin/valgrind --tool=pmemcheck "
            f"--trace-children=yes --trace-children-skip=/bin/sh --error-exitcode=-1 "
            f"{cmd}",
            log_path=log_path,
        )
        with open(log_path, "r") as f:
            if "ERROR SUMMARY: 0 errors" not in f.read():
                logger.error(f"pmemcheck failed. See {log_path}")
                exit(1)

    def _run_profile(self, cmd, log_path):
        perf_data = self.result_dir / "perf.data"
        flamegraph_output = self.result_dir / "flamegraph.svg"
        flamegraph_dir = self.build_path / "_deps" / "flamegraph-src"

        # record perf data
        system(
            f"perf record --freq=997 "
            f"--call-graph dwarf "  # options: fp, lbr, dwarf
            f"-o {perf_data} {cmd}",
            log_path=log_path,
        )

        # show perf results in terminal
        # system(f"perf report -i {perf_data}")

        # generate flamegraph
        system(
            f"perf script -i {perf_data} | "
            f"{flamegraph_dir}/stackcollapse-perf.pl | "
            f"{flamegraph_dir}/flamegraph.pl > {flamegraph_output}"
        )
        logger.info(f"The flamegraph is available at `{flamegraph_output}`")

    def _run_trace(self, cmd, log_path):
        trace_output = self.result_dir / "trace.fxt"
        prog = cmd.split(" ")[0]
        args = cmd.split(" ")[1:]
        # system(
        #     f"sudo PATH=$PATH magic-trace run -trace-include-kernel -o {trace_output}"
        #     f" {prog} -- {' '.join(args)}",
        #     log_path=log_path,
        # )
        system(
            f"magic-trace run -o {trace_output}"
            f" {prog} -- {' '.join(args)}",
            log_path=log_path,
        )
