#include <benchmark/benchmark.h>
#include <sys/stat.h>

#include "common.h"
#include "posix.h"

int num_iter = get_num_iter(10000);
const char* filepath = get_filepath();

int fd;

void bench(benchmark::State& state) {
  using namespace ulayfs;

  // Prepare file
  {
    unlink(filepath);

    fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) state.SkipWithError("open failed");

    auto file_size = state.range(0);
    prefill_file(fd, file_size, 4096);

    struct stat st;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    posix::fstat(fd, &st);
    state.counters["file_size"] =
        benchmark::Counter(st.st_size, benchmark::Counter::kDefaults,
                           benchmark::Counter::OneK::kIs1024);

    close(fd);
  }

  if (is_linked()) debug::clear_counts();
  for (auto _ : state) {
    fd = open(filepath, O_RDONLY);
    assert(fd >= 0);
    close(fd);
  }

  if (is_linked()) {
    debug::print_counter();
    state.counters["syscall"] = debug::get_duration(Event::OPEN_SYS).count();
    state.counters["mmap"] = debug::get_duration(Event::MMAP).count();
    state.counters["update"] = debug::get_duration(Event::UPDATE).count();
    debug::clear_counts();
  }
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  RegisterBenchmark("file_size", bench)
      ->RangeMultiplier(2)
      ->Arg(2 * 1024 * 1024 - 4096 * 2)
      ->Arg(4 * 1024 * 1024 - 4096 * 3)
      ->Arg(8 * 1024 * 1024 - 4096 * 5)
      ->Arg(16 * 1024 * 1024 - 4096 * 9)
      ->Arg(32 * 1024 * 1024 - 4096 * 17)
      ->Arg(64 * 1024 * 1024 - 4096 * 33)
      ->Iterations(num_iter);

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
