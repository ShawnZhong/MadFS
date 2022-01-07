#include "common.h"

enum class Mode {
  NO_OVERLAP,
  APPEND,
  CNCR_WRITE,  // concurrent write to the same location
  SRMW,        // single reader multiple concurrent writers to the same location
};

template <Mode mode, int READ_PERCENT = -1>
void bench(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  [[maybe_unused]] char dst_buf[MAX_SIZE * MAX_NUM_THREAD];
  [[maybe_unused]] char src_buf[MAX_SIZE * MAX_NUM_THREAD];
  std::fill(src_buf, src_buf + MAX_SIZE * MAX_NUM_THREAD, 'x');

  if (state.thread_index == 0) {
    unlink(FILEPATH);
    fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

    // preallocate file
    if constexpr (mode != Mode::APPEND) {
      write(fd, src_buf, num_bytes * MAX_NUM_THREAD);
      fsync(fd);
    }
  }

  // prepare random offset
  [[maybe_unused]] bool is_read[num_iter];
  if constexpr (mode == Mode::NO_OVERLAP) {
    std::generate(is_read, is_read + num_iter,
                  [&]() { return rand() % 100 < READ_PERCENT; });
  }

  // run benchmark
  if constexpr (mode == Mode::APPEND) {
    for (auto _ : state) {
      write(fd, src_buf, num_bytes);
      fsync(fd);
    }
  } else if constexpr (mode == Mode::CNCR_WRITE) {
    for (auto _ : state) {
      pwrite(fd, src_buf, num_bytes, 0);
      fsync(fd);
    }
  } else if constexpr (mode == Mode::SRMW) {
    if (state.thread_index == 0) {
      for (auto _ : state) {
        pread(fd, dst_buf, num_bytes, 0);
      }
    } else {
      for (auto _ : state) {
        pwrite(fd, src_buf, num_bytes, 0);
        fsync(fd);
      }
    }
  } else if constexpr (mode == Mode::NO_OVERLAP) {
    int i = 0;
    for (auto _ : state) {
      if (is_read[i++]) {
        pread(fd, dst_buf, num_bytes, 0);
      } else {
        pwrite(fd, src_buf, num_bytes, 0);
        fsync(fd);
      }
    }
  }

  // report result
  auto items_processed = static_cast<int64_t>(state.iterations());
  auto bytes_processed = items_processed * num_bytes;
  if constexpr (mode == Mode::SRMW) {
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

template <class F>
auto register_bm(const char* name, F f, int num_bytes = 4096) {
  return benchmark::RegisterBenchmark(name, f)
      ->Args({num_bytes})
      ->Threads(1)
      ->DenseThreadRange(2, MAX_NUM_THREAD, 2)
      ->Iterations(num_iter)
      ->UseRealTime();
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  if (auto str = std::getenv("BENCH_NUM_ITER"); str) num_iter = std::stoi(str);

  register_bm("no_overlap_0R", bench<Mode::NO_OVERLAP, 0>);
  register_bm("no_overlap_50R", bench<Mode::NO_OVERLAP, 50>);
  register_bm("no_overlap_95R", bench<Mode::NO_OVERLAP, 95>);
  register_bm("no_overlap_100R", bench<Mode::NO_OVERLAP, 100>);

  register_bm("append", bench<Mode::APPEND>, 512);
  register_bm("append", bench<Mode::APPEND>);
  register_bm("cncr_write", bench<Mode::CNCR_WRITE>);
  register_bm("srmw", bench<Mode::SRMW>);

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
