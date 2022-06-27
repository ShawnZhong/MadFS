#include "common.h"

#ifdef NDEBUG
constexpr static bool debug = false;
#else
constexpr static bool debug = true;
#endif

constexpr int MAX_SIZE = 4096;
constexpr int MAX_NUM_THREAD = 16;

int fd;
int num_iter = get_num_iter();

enum class Mode {
  NO_OVERLAP,
  APPEND,
  COW,
};

template <Mode mode, int READ_PERCENT = -1>
void bench(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  [[maybe_unused]] char dst_buf[MAX_SIZE * MAX_NUM_THREAD];
  [[maybe_unused]] char src_buf[MAX_SIZE * MAX_NUM_THREAD];
  std::fill(src_buf, src_buf + MAX_SIZE * MAX_NUM_THREAD, 'x');

  if (state.thread_index == 0) {
    unlink(filepath);
    fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) state.SkipWithError("open failed");

    // preallocate file
    if constexpr (mode != Mode::APPEND) {
      auto len = num_bytes * MAX_NUM_THREAD;
      [[maybe_unused]] ssize_t res = write(fd, src_buf, len);
      assert(res == len);
      fsync(fd);
    }
  }

  // run benchmark
  if constexpr (mode == Mode::APPEND) {
    for (auto _ : state) {
      [[maybe_unused]] ssize_t res = write(fd, src_buf, num_bytes);
      assert(res == num_bytes);
      fsync(fd);
    }
    if constexpr (debug) {
      if (state.thread_index == 0) {
        [[maybe_unused]] ssize_t res = lseek(fd, 0, SEEK_SET);
        for (int i = 0; i < num_iter; i++) {
          res = read(fd, dst_buf, num_bytes);
          if (res != num_bytes) {
            fprintf(stderr, "expected = %ld, actual = %ld\n",
                    num_iter * num_bytes, i * num_bytes);
            break;
          }
        }
      }
    }
  } else if constexpr (mode == Mode::COW) {
    for (auto _ : state) {
      [[maybe_unused]] ssize_t res = pwrite(fd, src_buf, num_bytes, 0);
      assert(res == num_bytes);
      fsync(fd);
    }
  } else if constexpr (mode == Mode::NO_OVERLAP) {
    bool is_read[num_iter];
    std::generate(is_read, is_read + num_iter,
                  [&]() { return rand() % 100 < READ_PERCENT; });
    const off_t offset = state.thread_index * num_bytes;
    int i = 0;
    for (auto _ : state) {
      if (is_read[i++]) {
        [[maybe_unused]] ssize_t res = pread(fd, dst_buf, num_bytes, offset);
        assert(res == num_bytes);
        assert(memcmp(dst_buf, src_buf, num_bytes) == 0);
      } else {
        [[maybe_unused]] ssize_t res = pwrite(fd, src_buf, num_bytes, offset);
        assert(res == num_bytes);
        fsync(fd);
      }
    }
  }

  // report result
  auto items_processed = static_cast<int64_t>(state.iterations());
  auto bytes_processed = items_processed * num_bytes;
  state.SetBytesProcessed(bytes_processed);
  state.SetItemsProcessed(items_processed);

  // tear down
  if (state.thread_index == 0) {
    close(fd);
    unlink(filepath);
  }
}

template <class F>
auto register_bm(const char* name, F f, int num_bytes = 4096) {
  return benchmark::RegisterBenchmark(name, f)
      ->Args({num_bytes})
      ->DenseThreadRange(1, MAX_NUM_THREAD)
      ->Iterations(num_iter)
      ->UseRealTime();
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  register_bm("no_overlap_0R", bench<Mode::NO_OVERLAP, 0>);
  register_bm("no_overlap_50R", bench<Mode::NO_OVERLAP, 50>);
  register_bm("no_overlap_95R", bench<Mode::NO_OVERLAP, 95>);
  register_bm("no_overlap_100R", bench<Mode::NO_OVERLAP, 100>);

  register_bm("append_512", bench<Mode::APPEND>, 512);
  register_bm("append_4k", bench<Mode::APPEND>);
  register_bm("cow_512", bench<Mode::COW>, 512);
  register_bm("cow_3584", bench<Mode::COW>, 3584);

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
