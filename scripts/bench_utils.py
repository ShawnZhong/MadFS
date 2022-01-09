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


def get_ulayfs_config():
    for path in ["/mnt/pmem0-ext4-dax", "."]:
        pmem_path = Path(path)
        if pmem_path.exists():
            return Filesystem("uLayFS", pmem_path=pmem_path, load_ulayfs=True)


def get_fs_configs():
    yield get_ulayfs_config()

    for (name, path) in [
        ("ext4-dax", "/mnt/pmem0-ext4-dax"),
        ("ext4-journal", "/mnt/pmem0-ext4-journal"),
        ("NOVA", "/mnt/pmem0-nova"),
    ]:
        pmem_path = Path(path)
        if not pmem_path.exists():
            logger.warning(f"{pmem_path} does not exist, skipping {name}")
            continue
        yield Filesystem(name, pmem_path)
