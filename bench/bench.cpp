#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include "common.h"

constexpr char FILEPATH[] = "test.txt";
constexpr int MAX_SIZE = 64 * 4096;
constexpr int MAX_NUM_THREAD = 16;

int fd;

enum class BenchMode {
  APPEND,
  OVERWRITE,
  READ,
  READ_WRITE,  // single reader multiple writers
  OPEN_CLOSE,
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
template <BenchMode mode>
void bench(benchmark::State& state) {
  // set up
  pin_node(0);
  if (state.thread_index == 0) {
    unlink(FILEPATH);
    fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  }

  [[maybe_unused]] char src_buf[MAX_SIZE];
  [[maybe_unused]] char dst_buf[MAX_SIZE];
  const auto num_bytes = state.range(0);
  if constexpr (mode != BenchMode::APPEND) {
    // allocate some space to the file for pread/pwrite
    write(fd, src_buf, num_bytes);
  }

  // run benchmark
  if constexpr (mode == BenchMode::APPEND) {
    for (auto _ : state) write(fd, src_buf, num_bytes);
  } else if constexpr (mode == BenchMode::OVERWRITE) {
    for (auto _ : state) pwrite(fd, src_buf, num_bytes, 0);
  } else if constexpr (mode == BenchMode::READ) {
    for (auto _ : state) pread(fd, dst_buf, num_bytes, 0);
  } else if constexpr (mode == BenchMode::READ_WRITE) {
    if (state.thread_index == 0) {
      for (auto _ : state) pread(fd, dst_buf, num_bytes, 0);
    } else {
      for (auto _ : state) pwrite(fd, src_buf, num_bytes, 0);
    }
  }

  // report result
  auto items_processed = static_cast<int64_t>(state.iterations());
  auto bytes_processed = items_processed * num_bytes;
  if constexpr (mode == BenchMode::READ_WRITE) {
    // for read-write, we only report the result of the reader thread
    if (state.thread_index == 0) {
      state.SetBytesProcessed(bytes_processed);
      state.SetItemsProcessed(items_processed);
    }
  } else {
    state.SetBytesProcessed(bytes_processed);
    state.SetItemsProcessed(items_processed);
  }

  // tear down
  if (state.thread_index == 0) {
    close(fd);
    unlink(FILEPATH);
  }
}

template <>
void bench<BenchMode::OPEN_CLOSE>(benchmark::State& state) {
  unlink(FILEPATH);
  close(open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR));
  for (auto _ : state) close(open(FILEPATH, O_RDWR));
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
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
           RegisterBenchmark("read_write", bench<BenchMode::READ_WRITE>),
       }) {
    bm->RangeMultiplier(2)
        ->Range(8, MAX_SIZE)
        ->Threads(1)
        ->DenseThreadRange(2, MAX_NUM_THREAD, 2)
        ->Iterations(num_iter)
        ->UseRealTime();
  }

  RegisterBenchmark("open_close", bench<BenchMode::OPEN_CLOSE>)
      ->Iterations(num_iter);

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
