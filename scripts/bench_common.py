import logging
from dataclasses import dataclass
from pathlib import Path

from runner import build_types
from utils import system

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bench")


def drop_cache():
    system("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null")


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


ext4_dax_path = get_ext4_dax_path()
_fs_configs = [
    ("uLayFS", ext4_dax_path, True),
    ("ext4", ext4_dax_path, False),
    ("NOVA", "/mnt/pmem0-nova", False),
]
fs_names = [fs[0] for fs in _fs_configs]


def get_fs_configs(fs_names):
    for (name, path, load_ulayfs) in _fs_configs:
        if name not in fs_names:
            continue
        pmem_path = Path(path)
        if not pmem_path.exists():
            logger.warning(f"{pmem_path} does not exist, skipping {name}")
            continue
        yield Filesystem(name, pmem_path, load_ulayfs)


def add_common_args(argparser):
    argparser.add_argument(
        "-p",
        "--result_dir",
        type=Path,
        help="If set, plot the results in the given directory w/o running the benchmark",
    )
    argparser.add_argument(
        "-b",
        "--build_type",
        default="release",
        choices=build_types,
    )
    argparser.add_argument(
        "-f",
        "--fs_names",
        default=fs_names,
        choices=fs_names,
        nargs="+",
        help="Filesystems to benchmark",
    )
