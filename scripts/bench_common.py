import logging
from dataclasses import dataclass
from pathlib import Path

from runner import build_types
from utils import system

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench")


@dataclass
class Filesystem:
    name: str
    pmem_path: Path
    load_ulayfs: bool = False


def get_ext4_dax_path():
    for path in ["/mnt/pmem0-ext4-dax", "/mnt/pmem0", "/mnt/pmem1"]:
        if Path(path).exists():
            return Path(path)

    logger.warning(f"Cannot find ext4-dax path, use current directory")
    return Path(".")


def get_fs_configs():
    ext4_dax_path = get_ext4_dax_path()

    for (name, path, load_ulayfs) in [
        ("uLayFS", ext4_dax_path, True),
        ("ext4", ext4_dax_path, False),
        ("NOVA", "/mnt/pmem0-nova", False),
    ]:
        pmem_path = Path(path)
        if not pmem_path.exists():
            logger.warning(f"{pmem_path} does not exist, skipping {name}")
            continue
        yield Filesystem(name, pmem_path, load_ulayfs)


def drop_cache():
    system("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null")


def add_common_args(argparser):
    argparser.add_argument(
        "-p",
        dest="result_dir",
        type=Path,
        help="If set, plot the results in the given directory w/o running the benchmark",
    )
    argparser.add_argument(
        "-b",
        "--build_type",
        default="release",
        choices=build_types,
    )
