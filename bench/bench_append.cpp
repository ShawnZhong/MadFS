#include "common.h"

constexpr const char buf[MAX_SIZE]{};

int fd;

static void bench(benchmark::State& state) {
  pin_node(0);
  if (state.thread_index() == 0) {
    remove(FILEPATH);
    fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  }

  const auto num_bytes = state.range(0);
  for (auto _ : state) {
    ssize_t res = write(fd, buf, num_bytes);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * num_bytes);

  if (state.thread_index() == 0) {
    close(fd);
    remove(FILEPATH);
  }
}

BENCHMARK(bench)
    ->RangeMultiplier(2)
    ->Range(8, MAX_SIZE)
    ->Threads(1)
    ->DenseThreadRange(2, MAX_NUM_THREAD, 2)
    ->Iterations(NUM_ITER)
    ->UseRealTime();
BENCHMARK_MAIN();
