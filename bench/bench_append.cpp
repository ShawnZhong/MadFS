#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

constexpr char FILEPATH[] = "test.txt";
constexpr int MAX_SIZE = 64 * 4096;
constexpr const char buf[MAX_SIZE]{};

int fd;

static void bench(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  if (state.thread_index() == 0) {
    remove(FILEPATH);
    fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  }

  for (auto _ : state) {
    ssize_t res = write(fd, buf, num_bytes);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * num_bytes);

  if (state.thread_index() == 0) {
    close(fd);
  }
}

BENCHMARK(bench)
    ->RangeMultiplier(2)
    ->Range(8, MAX_SIZE)
    ->ThreadRange(1, 16)
    ->Iterations(1000)
    ->UseRealTime();
BENCHMARK_MAIN();
