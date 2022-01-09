import logging
import pprint
import shutil

from utils import get_timestamp, system, root_dir

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("runner")


class Runner:
    def __init__(self, cmake_target, build_type=None, result_dir=None):
        self.is_micro = cmake_target.startswith("micro")
        self.is_bench = self.is_micro or cmake_target.startswith("leveldb") or cmake_target.startswith("tpcc")

        if build_type is None:
            build_type = "release" if self.is_bench else "debug"
        if result_dir is None:
            result_dir = root_dir / "results" / cmake_target / build_type / get_timestamp()

        self.build_type = build_type
        self.build_path = root_dir / f"build-{build_type}"
        self.cmake_target = cmake_target
        self.result_dir = result_dir
        self.exe_path = None
        self.ulayfs_path = None
        self.bm_output_path = None

        self.result_dir.mkdir(parents=True, exist_ok=True)

    def build(self, cmake_args=""):
        if self.is_bench:
            cmake_args += " -DULAYFS_BUILD_BENCH=ON "

        # build
        build_log_path = self.result_dir / "build.log"
        system(
            f"make {self.build_type} -C {root_dir} "
            f"CMAKE_ARGS='{cmake_args}' "
            f"BUILD_TARGETS='{self.cmake_target} ulayfs' ",
            log_path=build_log_path,
        )

        # save config
        config_log_path = self.result_dir / "config.log"
        with open(config_log_path, "w") as fout:
            pprint.pprint(locals(), stream=fout)
        system(f"cmake -LA -N {self.build_path} >> {config_log_path}")

        self.ulayfs_path = self.build_path / "libulayfs.so"
        self.exe_path = self.build_path / self.cmake_target

    def run(self, prog_args="", load_ulayfs=True, numa=0, pmem_path=None):
        assert self.exe_path is not None

        if self.is_micro:
            self.bm_output_path = self.result_dir / "result.json"
            prog_args += f" --benchmark_counters_tabular=true "
            prog_args += f" --benchmark_out={self.bm_output_path} "
        cmd = f"{self.exe_path} {prog_args}"

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
        prog_log_path = self.result_dir / "prog.log"
        if self.build_type == "pmemcheck":
            pmemcheck_dir = self.build_path / "_deps" / "pmemcheck-src"
            system(
                f"VALGRIND_LIB={pmemcheck_dir}/libexec/valgrind/ "
                f"{pmemcheck_dir}/bin/valgrind --tool=pmemcheck --trace-children=yes "
                f"{cmd}",
                log_path=prog_log_path,
            )

        elif self.build_type == "profile":
            perf_data = self.result_dir / "perf.data"
            flamegraph_output = self.result_dir / "flamegraph.svg"
            flamegraph_dir = self.build_path / "_deps" / "flamegraph-src"

            # record perf data
            system(
                f"perf record --freq=997 --call-graph dwarf -o {perf_data} {cmd}",
                log_path=prog_log_path,
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
            system(cmd, log_path=prog_log_path)
