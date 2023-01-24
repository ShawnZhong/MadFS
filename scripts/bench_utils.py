from fs import available_fs
from init import init
from runner import Runner
from utils import system


def run_gbench(cmake_target, build_type, cmake_args, fs_names, run_config, result_dir):
    init()
    for fs_name in fs_names:
        fs = available_fs[fs_name]
        runner = Runner(
            cmake_target,
            result_dir=result_dir / fs_name,
            build_type=build_type
        )
        runner.build(cmake_args=cmake_args)
        if fs.path:
            system(f"rm -rf {fs.path}/*")
        runner.run(fs=fs, **run_config)
    return result_dir


def drop_cache(num=3):
    system(f"echo {num} | sudo tee /proc/sys/vm/drop_caches >/dev/null")
