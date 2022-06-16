import logging
import shutil
from pathlib import Path
from typing import Dict, Optional, List

from utils import root_dir, is_ulayfs_linked

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fs")


def infer_numa_node(pmem_path: Path):
    if "pmem" in pmem_path.name:
        numa_str = pmem_path.name.partition("pmem")[2][0]
        if numa_str.isnumeric():
            return int(numa_str)

    logger.warning(f"Cannot infer numa node for {pmem_path}, assuming 0")
    return 0


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
        env = {
            **env,
            **self.get_env(cmd=cmd, **kwargs),
            "PMEM_PATH": str(self.path),
        }

        cmd = ["env"] + [f"{k}={v}" for k, v in env.items()] + cmd

        numa = infer_numa_node(self.path)
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
    def get_env(cmd, build_type):
        if is_ulayfs_linked(cmd[0]):
            return {}
        ulayfs_path = root_dir / f"build-{build_type}" / "libulayfs.so"
        if not ulayfs_path.exists():
            logger.warning(f"Cannot find ulayfs path: {ulayfs_path}")
            return {}
        env = {"LD_PRELOAD": ulayfs_path}
        return env


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


def get_available_fs() -> Dict[str, Filesystem]:
    all_fs = [ULAYFS(), Ext4DAX(), NOVA(), SplitFS()]
    return {fs.name: fs for fs in all_fs if fs.is_available()}


available_fs = get_available_fs()
