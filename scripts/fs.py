import logging
import shutil
from pathlib import Path
from typing import Dict, Optional, List

from utils import root_dir, is_ulayfs_linked, system

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fs")


def infer_numa_node(pmem_path: Path):
    if "pmem" in pmem_path.name:
        numa_str = pmem_path.name.partition("pmem")[2][0]
        if numa_str.isnumeric():
            return int(numa_str)

    logger.warning(f"Cannot infer numa node for {pmem_path}, assuming 0")
    return 0


def get_cpulist(numa_node: int) -> List[int]:
    result = []
    text = Path(f"/sys/devices/system/node/node{numa_node}/cpulist").read_text()
    for r in text.split(","):
        start, end = r.split("-")
        result += list(range(int(start), int(end) + 1))
    return result


class Filesystem:
    name: str

    @property
    def path(self) -> Optional[Path]:
        raise NotImplementedError()

    def is_available(self) -> bool:
        return self.path is not None

    def get_env(self, **kwargs):
        return {}

    def process_cmd(self, cmd: List[str], env: Dict[str, str], **kwargs) -> List[str]:
        numa = infer_numa_node(self.path)

        env = {
            **env,
            **self.get_env(cmd=cmd, **kwargs),
            "PMEM_PATH": str(self.path),
            "CPULIST": ",".join(str(c) for c in get_cpulist(numa)),
        }

        cmd = ["env"] + [f"{k}={v}" for k, v in env.items()] + cmd

        if shutil.which("numactl"):
            cmd = ["numactl", f"--cpunodebind={numa}", f"--membind={numa}"] + cmd
        else:
            logger.warning("numactl not found, NUMA not enabled")

        return cmd


def _get_ext4_path() -> Path:
    for path in ["/mnt/pmem0-ext4-dax", "/mnt/pmem0", "/mnt/pmem1", "/mnt/pmem_emul"]:
        if Path(path).is_mount():
            return Path(path)

    logger.warning(f"Cannot find ext4-dax path, use current directory")
    return Path(".")


_ext4_path = _get_ext4_path()


class Ext4DAX(Filesystem):
    name = "ext4-DAX"

    @property
    def path(self) -> Optional[Path]:
        return _ext4_path


class ULAYFS(Ext4DAX):
    name = "uLayFS"
    cc = "OCC"

    @staticmethod
    def build(build_type, result_dir, cmake_args=""):
        build_path = root_dir / f"build-{build_type}"

        system(
            f"make {build_type} -C {root_dir} "
            f"CMAKE_ARGS='{cmake_args}' "
            f"BUILD_TARGETS='ulayfs' ",
            log_path=result_dir / "build.log",
        )

        config_log_path = result_dir / "config.log"
        system(f"cmake -LA -N {build_path} >> {config_log_path}")

        return build_path / "libulayfs.so"

    @staticmethod
    def _make_cmake_args(cc: str) -> str:
        options = ["OCC", "MUTEX", "SPINLOCK", "RWLOCK"]

        if cc not in options:
            raise ValueError(f"{cc} is not a valid option")

        result = " ".join(
            f"-DULAYFS_CC_{option}={'ON' if option == cc else 'OFF'}"
            for option in options
        )

        return result

    def get_env(self, cmd, **kwargs):
        if is_ulayfs_linked(cmd[0]):
            return {}
        cmake_args = ULAYFS._make_cmake_args(self.cc)
        ulayfs_path = ULAYFS.build(cmake_args=cmake_args, **kwargs)
        env = {"LD_PRELOAD": ulayfs_path}
        return env


class ULAYFS_OCC(ULAYFS):
    name = "OCC"
    cc = "OCC"


class ULAYFS_MUTEX(ULAYFS):
    name = "mutex"
    cc = "MUTEX"


class ULAYFS_SPINLOCK(ULAYFS):
    name = "spinlock"
    cc = "SPINLOCK"


class ULAYFS_RWLOCK(ULAYFS):
    name = "rwlock"
    cc = "RWLOCK"


class NOVA(Filesystem):
    name = "NOVA"

    @property
    def path(self) -> Optional[Path]:
        path = Path("/mnt/pmem0-nova")
        if path.is_mount():
            return path
        return None


class SplitFS(Filesystem):
    name = "SplitFS"
    build_path = Path.home() / "SplitFS" / "splitfs"

    @property
    def path(self) -> Optional[Path]:
        path = Path("/mnt/pmem_emul")
        if path.is_mount():
            return path
        return None

    def is_available(self) -> bool:
        if self.path is None:
            return False

        if not self.build_path.exists():
            logger.warning(f"Cannot find SplitFS path: {self.build_path}")
            return False

        return True

    def get_env(self, **kwargs):
        env = {
            "LD_LIBRARY_PATH": SplitFS.build_path,
            "NVP_TREE_FILE": SplitFS.build_path / "bin" / "nvp_nvp.tree",
            "LD_PRELOAD": SplitFS.build_path / "libnvp.so"
        }
        return env


all_bench_fs = {fs.name: fs for fs in [ULAYFS(), Ext4DAX(), NOVA(), SplitFS()]}
all_extra_fs = {fs.name: fs for fs in [ULAYFS_OCC(), ULAYFS_SPINLOCK(), ULAYFS_MUTEX(), ULAYFS_RWLOCK()]}
all_fs = {**all_bench_fs, **all_extra_fs}

bench_fs = {k: v for k, v in all_bench_fs.items() if v.is_available()}
available_fs = {**bench_fs, **{k: v for k, v in all_extra_fs.items() if v.is_available()}}
