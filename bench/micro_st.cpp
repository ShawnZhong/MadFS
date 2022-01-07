#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include "common.h"

constexpr char FILEPATH[] = "test.txt";
constexpr int MAX_SIZE = 128 * 1024;

int fd;
int num_iter = 10000;

enum class Mode {
  APPEND,
  SEQ_READ,
  SEQ_WRITE,
  RND_READ,
  RND_WRITE,
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
template <Mode mode>
void bench(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  unlink(FILEPATH);
  fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  [[maybe_unused]] char dst_buf[MAX_SIZE];
  [[maybe_unused]] char src_buf[MAX_SIZE];
  std::fill(src_buf, src_buf + MAX_SIZE, 'x');

  // prepare random offset
  [[maybe_unused]] int rand_off[num_iter];
  if constexpr (mode == Mode::RND_READ || mode == Mode::RND_WRITE) {
    std::generate(rand_off, rand_off + num_iter,
                  [&]() { return rand() % num_iter * num_bytes; });
  }

  // preallocate file
  if constexpr (mode != Mode::APPEND) {
    auto buf = new char[num_bytes * num_iter];
    pwrite(fd, buf, num_bytes * num_iter, 0);
    delete[] buf;
  }

  // run benchmark
  if constexpr (mode == Mode::APPEND) {
    for (auto _ : state) write(fd, src_buf, num_bytes);
  } else if constexpr (mode == Mode::SEQ_READ) {
    for (auto _ : state) read(fd, dst_buf, num_bytes);
  } else if constexpr (mode == Mode::SEQ_WRITE) {
    for (auto _ : state) write(fd, src_buf, num_bytes);
  } else if constexpr (mode == Mode::RND_READ) {
    for (auto _ : state)
      pread(fd, dst_buf, num_bytes, rand_off[state.iterations()]);
  } else if constexpr (mode == Mode::RND_WRITE) {
    for (auto _ : state)
      pwrite(fd, src_buf, num_bytes, rand_off[state.iterations()]);
  }

  // report result
  auto items_processed = static_cast<int64_t>(state.iterations());
  auto bytes_processed = items_processed * num_bytes;
  state.SetBytesProcessed(bytes_processed);
  state.SetItemsProcessed(items_processed);

  // tear down
  close(fd);
  unlink(FILEPATH);
}
#pragma GCC diagnostic pop

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  if (auto str = std::getenv("BENCH_NUM_ITER"); str) num_iter = std::stoi(str);

  for (auto& bm : {
           RegisterBenchmark("seq_read", bench<Mode::SEQ_READ>),
           RegisterBenchmark("rnd_read", bench<Mode::RND_READ>),
           RegisterBenchmark("seq_write", bench<Mode::SEQ_WRITE>),
           RegisterBenchmark("rnd_write", bench<Mode::RND_WRITE>),
           RegisterBenchmark("append", bench<Mode::APPEND>),
       }) {
    bm->RangeMultiplier(2)->Range(8, MAX_SIZE)->Iterations(num_iter);
  }

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
