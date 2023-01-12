from fs import available_fs
from runner import Runner
from utils import system, init


class Benchmark:
    def __init__(self):
        init(configure=True)

    def build_and_run(self, cmake_target, build_type, cmake_args, fs_names, run_config, result_dir):
        for fs_name in fs_names:
            fs = available_fs[fs_name]
            runner = Runner(
                cmake_target,
                result_dir=result_dir / fs_name,
                build_type=build_type
            )
            runner.build(cmake_args=cmake_args)
            self.run_single_config(
                fs=fs,
                runner=runner,
                run_config=run_config
            )
        return result_dir

    def run_single_config(self, fs, runner: Runner, run_config):
        if fs.path:
            system(f"rm -rf {fs.path}/*")
        runner.run(fs=fs, **run_config)


def drop_cache(num=3):
    system(f"echo {num} | sudo tee /proc/sys/vm/drop_caches >/dev/null")
