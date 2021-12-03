import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("utils")


def system(cmd, log_path=None):
    import os
    import sys

    if log_path:
        cmd = f"({cmd}) 2>&1 | tee -a {log_path}"

    logger.info(f"Executing command: `{cmd}`")
    ret = os.system(f'/bin/bash -o pipefail -c "{cmd}"')
    if ret != 0:
        sys.exit(f"Command `{cmd}` failed with return code {ret}")


def check_any_args_passed(parser):
    import sys

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()


def get_timestamp():
    import datetime

    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
