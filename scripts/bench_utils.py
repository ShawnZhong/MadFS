import logging
from dataclasses import dataclass
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench")


@dataclass
class Filesystem:
    name: str
    pmem_path: Path
    load_ulayfs: bool = False
    cmake_args: str = ""


def get_ext4_dax_path():
    pmem_path = Path("/mnt/pmem0-ext4-dax")
    if pmem_path.exists():
        return pmem_path

    logger.warning(f"{pmem_path} does not exist, use current directory")
    return Path(".")


def get_fs_configs():
    ext4_dax_path = get_ext4_dax_path()

    yield Filesystem("uLayFS-native", pmem_path=ext4_dax_path, load_ulayfs=True, cmake_args="-DULAYFS_PERSIST=NATIVE")
    yield Filesystem("uLayFS-pmdk", pmem_path=ext4_dax_path, load_ulayfs=True, cmake_args="-DULAYFS_PERSIST=PMDK")
    yield Filesystem("uLayFS-kernel", pmem_path=ext4_dax_path, load_ulayfs=True, cmake_args="-DULAYFS_PERSIST=KERNEL")

    for (name, path) in [
        ("ext4-dax", ext4_dax_path),
        ("ext4-journal", "/mnt/pmem0-ext4-journal"),
        ("NOVA", "/mnt/pmem0-nova"),
    ]:
        pmem_path = Path(path)
        if not pmem_path.exists():
            logger.warning(f"{pmem_path} does not exist, skipping {name}")
            continue
        yield Filesystem(name, pmem_path)
