#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

constexpr char FILEPATH[] = "test.txt";

static void bench_rw(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  char buf[num_bytes];
  for (auto _ : state) {
    ssize_t res = write(fd, buf, num_bytes);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          state.range(0));
}

BENCHMARK(bench_rw)->RangeMultiplier(2)->Range(8, 4096 * 64)->Iterations(10000);
BENCHMARK_MAIN();
