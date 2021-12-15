#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include "common.h"

constexpr char FILEPATH[] = "test.txt";
constexpr int MAX_SIZE = 64 * 4096;
constexpr int MAX_NUM_THREAD = 16;
constexpr const char src_buf[MAX_SIZE]{};

int fd;

enum class BenchMode {
  APPEND,
  OVERWRITE,
  READ,
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
template <BenchMode mode>
static void bench(benchmark::State& state) {
  pin_node(0);
  if (state.thread_index() == 0) {
    remove(FILEPATH);
    fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  }

  const auto num_bytes = state.range(0);
  if constexpr (mode == BenchMode::APPEND) {
    for (auto _ : state) write(fd, src_buf, num_bytes);
  } else if constexpr (mode == BenchMode::OVERWRITE) {
    for (auto _ : state) pwrite(fd, src_buf, num_bytes, 0);
  } else if constexpr (mode == BenchMode::READ) {
    write(fd, src_buf, MAX_SIZE);
    char dst_buf[MAX_SIZE];
    for (auto _ : state) pread(fd, dst_buf, num_bytes, 0);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * num_bytes);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));

  if (state.thread_index() == 0) {
    close(fd);
    remove(FILEPATH);
  }
}
#pragma GCC diagnostic pop

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  int num_iter = 10000;
  if (auto str = std::getenv("BENCH_NUM_ITER"); str) num_iter = std::stoi(str);

  for (auto& bm : {
           RegisterBenchmark("append", bench<BenchMode::APPEND>),
           RegisterBenchmark("overwrite", bench<BenchMode::OVERWRITE>),
           RegisterBenchmark("read", bench<BenchMode::READ>),
       }) {
    bm->RangeMultiplier(2)
        ->Range(8, MAX_SIZE)
        ->Threads(1)
        ->DenseThreadRange(2, MAX_NUM_THREAD, 2)
        ->Iterations(num_iter)
        ->UseRealTime();
  }

  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
