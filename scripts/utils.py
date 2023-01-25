import logging
import os
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("utils")

root_dir = Path(__file__).parent.parent


def system(cmd, log_path=None):
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = f"({cmd}) 2>&1 | tee -a {log_path}"

    cmd = f'/bin/bash -o pipefail -c "{cmd}"'
    msg = f"Executing command: {cmd}"
    logger.info(msg)
    if log_path:
        with log_path.open("a") as f:
            f.write(msg + "\n")
    ret = os.system(cmd)
    if ret != 0:
        sys.exit(f"Command `{cmd}` failed with return code {ret}")


def get_timestamp():
    return datetime.now().strftime("%Y%m%d-%H%M%S")
