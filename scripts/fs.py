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

    @staticmethod
    def get_env(**kwargs):
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


class Ext4DAX(Filesystem):
    name = "ext4-DAX"

    @property
    def path(self) -> Optional[Path]:
        for path in ["/mnt/pmem0-ext4-dax", "/mnt/pmem0", "/mnt/pmem1", "/mnt/pmem_emul"]:
            if Path(path).is_mount():
                return Path(path)

        logger.warning(f"Cannot find ext4-dax path, use current directory")
        return Path(".")


class ULAYFS(Ext4DAX):
    name = "uLayFS"

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

    @staticmethod
    def _get_env(cmd, cc, **kwargs):
        if is_ulayfs_linked(cmd[0]):
            return {}
        cmake_args = ULAYFS._make_cmake_args(cc)
        ulayfs_path = ULAYFS.build(cmake_args=cmake_args, **kwargs)
        env = {"LD_PRELOAD": ulayfs_path}
        return env

    @staticmethod
    def get_env(cmd, **kwargs):
        return ULAYFS._get_env(cmd, cc="OCC", **kwargs)


class ULAYFS_OCC(ULAYFS):
    name = "OCC"


class ULAYFS_MUTEX(ULAYFS):
    name = "mutex"

    @staticmethod
    def get_env(cmd, **kwargs):
        return ULAYFS._get_env(cmd, cc="MUTEX", **kwargs)


class ULAYFS_SPINLOCK(ULAYFS):
    name = "spinlock"

    @staticmethod
    def get_env(cmd, **kwargs):
        return ULAYFS._get_env(cmd, cc="SPINLOCK", **kwargs)


class ULAYFS_RWLOCK(ULAYFS):
    name = "rwlock"

    @staticmethod
    def get_env(cmd, **kwargs):
        return ULAYFS._get_env(cmd, cc="RWLOCK", **kwargs)


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

    @staticmethod
    def get_env(**kwargs):
        env = {
            "LD_LIBRARY_PATH": SplitFS.build_path,
            "NVP_TREE_FILE": SplitFS.build_path / "bin" / "nvp_nvp.tree",
            "LD_PRELOAD": SplitFS.build_path / "libnvp.so"
        }
        return env


def filter_available_fs(fs_list: List[Filesystem]) -> Dict[str, Filesystem]:
    return {fs.name: fs for fs in fs_list if fs.is_available()}


_bench_fs = [ULAYFS(), Ext4DAX(), NOVA(), SplitFS()]
bench_fs = filter_available_fs(_bench_fs)
available_fs = filter_available_fs(_bench_fs + [ULAYFS_OCC(), ULAYFS_SPINLOCK(), ULAYFS_MUTEX(), ULAYFS_RWLOCK()])
