import logging
import shutil
from pathlib import Path
from typing import Dict

from utils import root_dir, is_ulayfs_linked

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fs")


def get_ext4_dax_path():
    for path in ["/mnt/pmem0-ext4-dax", "/mnt/pmem0", "/mnt/pmem1"]:
        if Path(path).is_mount():
            return Path(path)

    logger.warning(f"Cannot find ext4-dax path, use current directory")
    return Path(".")


def get_available_filesystems() -> Dict[str, Path]:
    ext4_dax_path = get_ext4_dax_path()
    result = {
        "uLayFS": ext4_dax_path,
        "ext4-dax": ext4_dax_path,
    }

    nova_path = Path("/mnt/pmem0-nova")
    if nova_path.is_mount():
        result["NOVA"] = nova_path

    return result


available_filesystems = get_available_filesystems()


def process_cmd(fs: str, cmd, build_type):
    if fs == "uLayFS" and not is_ulayfs_linked(cmd.split(" ")[0]):
        ulayfs_path = root_dir / f"build-{build_type}" / "libulayfs.so"
        if not ulayfs_path.exists():
            logger.warning(f"Cannot find ulayfs path: {ulayfs_path}")
        cmd = f"env LD_PRELOAD={ulayfs_path} {cmd}"

    pmem_path = available_filesystems[fs]
    cmd = f"env PMEM_PATH={pmem_path} {cmd}"

    if "pmem" in pmem_path.name:
        numa = int(pmem_path.name.partition("pmem")[2][0])
        if shutil.which("numactl"):
            cmd = f"numactl --cpunodebind={numa} --membind={numa} {cmd}"
        else:
            logger.warning("numactl not found, NUMA not enabled")

    return cmd
