import logging
import pprint
import shutil

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
    def __init__(self, name, build_type=None, result_dir=None):
        self.is_micro = name.startswith("micro")
        self.is_bench = any(name.startswith(x) for x in ["micro", "leveldb", "tpcc"])

        if build_type is None:
            build_type = "release" if self.is_bench else "debug"
        if result_dir is None:
            result_dir = root_dir / "results" / name / build_type / get_timestamp()

        self.name = name
        self.build_type = build_type
        self.build_path = root_dir / f"build-{build_type}"
        self.result_dir = result_dir
        self.prog_path = None
        self.ulayfs_path = None

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

        self.ulayfs_path = self.build_path / "libulayfs.so"
        self.prog_path = self.build_path / cmake_target

    def run(
            self,
            cmd=None,
            additional_args="",
            load_ulayfs=True,
            numa=0,
            pmem_path=None,
            prog_log_name="prog.log",
    ):
        if cmd is None:
            assert self.prog_path is not None
            cmd = f"{self.prog_path} "

        if self.is_micro:
            json_path = self.result_dir / "result.json"
            cmd += f" --benchmark_counters_tabular=true "
            cmd += f" --benchmark_out={json_path} "

        cmd = f"{cmd} {additional_args}"

        # setup envs
        env = {}
        if load_ulayfs:
            assert self.ulayfs_path is not None
            env["LD_PRELOAD"] = self.ulayfs_path
        if pmem_path:
            env["PMEM_PATH"] = pmem_path
        if env:
            cmd = f"env {' '.join(f'{k}={v}' for k, v in env.items())} {cmd}"

        if shutil.which("numactl"):
            cmd = f"numactl --cpunodebind={numa} --membind={numa} {cmd}"
        else:
            logger.warning("numactl not found, NUMA not enabled")

        # execute
        prog_log_path = self.result_dir / prog_log_name
        if self.build_type == "pmemcheck":
            self._run_pmemcheck(cmd, log_path=prog_log_path)
        elif self.build_type == "profile":
            self._run_profile(cmd, log_path=prog_log_path)
        else:
            system(cmd, log_path=prog_log_path)

    def _run_pmemcheck(self, cmd, log_path):
        pmemcheck_dir = self.build_path / "_deps" / "pmemcheck-src"
        system(
            f"VALGRIND_LIB={pmemcheck_dir}/libexec/valgrind/ "
            f"{pmemcheck_dir}/bin/valgrind --tool=pmemcheck --trace-children=yes "
            f"{cmd}",
            log_path=log_path,
        )

    def _run_profile(self, cmd, log_path):
        perf_data = self.result_dir / "perf.data"
        flamegraph_output = self.result_dir / "flamegraph.svg"
        flamegraph_dir = self.build_path / "_deps" / "flamegraph-src"

        # record perf data
        system(
            f"perf record --freq=997 --call-graph dwarf -o {perf_data} {cmd}",
            log_path=log_path,
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
