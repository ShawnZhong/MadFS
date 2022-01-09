import logging
from dataclasses import dataclass
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench")


@dataclass
class Filesystem:
    name: str
    pmem_path: Path
    load_ulayfs: bool


def get_fs_configs():
    for (name, path, load_ulayfs) in [
        ("uLayFS", "/mnt/pmem0-ext4-dax", True),
        ("ext4-dax", "/mnt/pmem0-ext4-dax", False),
        ("ext4-journal", "/mnt/pmem0-ext4-journal", False),
        ("NOVA", "/mnt/pmem0-nova", False),
    ]:
        pmem_path = Path(path)
        if not pmem_path.exists():
            logger.warning(f"{pmem_path} does not exist, skipping {name}")
            continue
        yield Filesystem(name, pmem_path, load_ulayfs)
