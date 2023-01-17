import logging
from pathlib import Path
from typing import Optional

from utils import root_dir, is_madfs_linked, system

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fs")


def infer_numa_node(pmem_path: Path) -> int:
    if "pmem" in pmem_path.name:
        numa_str = pmem_path.name.partition("pmem")[2]
        if len(numa_str) != 0 and numa_str[0].isnumeric():
            return int(numa_str[0])

    logger.warning(f"Cannot infer numa node for {pmem_path}, assuming 0")
    return 0


class Filesystem:
    name: str

    @property
    def path(self) -> Optional[Path]:
        raise NotImplementedError()

    def is_available(self) -> bool:
        return self.path is not None

    def get_env(self, **kwargs):
        return {}


def _get_ext4_path() -> Path:
    for path in [
        "/mnt/pmem0-ext4-dax",
        "/mnt/pmem",
        "/mnt/pmem0",
        "/mnt/pmem1",
        "/mnt/pmem_emul",
    ]:
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


class MADFS(Ext4DAX):
    name = "MadFS"
    cc = "OCC"

    @staticmethod
    def build(build_type, result_dir, cmake_args=""):
        build_path = root_dir / f"build-{build_type}"

        system(
            f"make {build_type} -C {root_dir} "
            f"CMAKE_ARGS='{cmake_args}' "
            f"BUILD_TARGETS='madfs' ",
            log_path=result_dir / "build.log",
        )

        config_log_path = result_dir / "config.log"
        system(f"cmake -LA -N {build_path} >> {config_log_path}")

        return build_path / "libmadfs.so"

    @staticmethod
    def _make_cmake_args(cc: str) -> str:
        options = ["OCC", "MUTEX", "SPINLOCK", "RWLOCK"]

        if cc not in options:
            raise ValueError(f"{cc} is not a valid option")

        result = " ".join(
            f"-DMADFS_CC_{option}={'ON' if option == cc else 'OFF'}"
            for option in options
        )

        return result

    def get_env(self, prog, **kwargs):
        if is_madfs_linked(prog):
            return {}
        cmake_args = MADFS._make_cmake_args(self.cc)
        madfs_path = MADFS.build(cmake_args=cmake_args, **kwargs)
        env = {"LD_PRELOAD": madfs_path}
        return env


class MADFS_OCC(MADFS):
    name = "OCC"
    cc = "OCC"


class MADFS_MUTEX(MADFS):
    name = "Mutex"
    cc = "MUTEX"


class MADFS_SPINLOCK(MADFS):
    name = "Spinlock"
    cc = "SPINLOCK"


class MADFS_RWLOCK(MADFS):
    name = "RwLock"
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
            "LD_PRELOAD": SplitFS.build_path / "libnvp.so",
        }
        return env


all_bench_fs = {fs.name: fs for fs in [MADFS(), Ext4DAX(), NOVA(), SplitFS()]}
all_extra_fs = {
    fs.name: fs
    for fs in [MADFS_OCC(), MADFS_SPINLOCK(), MADFS_MUTEX(), MADFS_RWLOCK()]
}
all_fs = {**all_bench_fs, **all_extra_fs}

bench_fs = {k: v for k, v in all_bench_fs.items() if v.is_available()}
available_fs = {
    **bench_fs,
    **{k: v for k, v in all_extra_fs.items() if v.is_available()},
}
