import datetime
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("utils")

root_dir = Path(__file__).parent.parent


def system(cmd, log_path=None):
    if log_path:
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


def check_any_args_passed(parser):
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()


def get_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
