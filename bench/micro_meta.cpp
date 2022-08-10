#include <sys/stat.h>

#include "common.h"

int fd;
int num_iter = get_num_iter(10000);

enum class Mode {
  OPEN_TX_LEN,
  OPEN_FILE_SIZE,
};

template <Mode mode>
void bench(benchmark::State& state) {
  unlink(filepath);

  fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  if (fd < 0) state.SkipWithError("open failed");

  if constexpr (mode == Mode::OPEN_TX_LEN) {
    char buf[4096];
    for (int i = 0; i < state.range(0); ++i) {
      [[maybe_unused]] ssize_t res = pwrite(fd, buf, sizeof(buf), 0);
      assert(res == sizeof(buf));
    }
  } else if constexpr (mode == Mode::OPEN_FILE_SIZE) {
    auto target_file_size = state.range(0);
    int num_ops = static_cast<int>(target_file_size / 4096 * 0.998);
    char buffer[4096];
    for (int i = 0; i < num_ops; ++i) {
      [[maybe_unused]] size_t res = write(fd, buffer, 4096);
      assert(res == 4096);
    }
    struct stat st;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    fstat(fd, &st);
    state.counters["file_size"] =
        benchmark::Counter(st.st_blocks * 512, benchmark::Counter::kDefaults,
                           benchmark::Counter::OneK::kIs1024);
  }

  close(fd);

  for (auto _ : state) {
    fd = open(filepath, O_RDONLY);
    assert(fd >= 0);
    close(fd);
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  RegisterBenchmark("open_tx_len", bench<Mode::OPEN_TX_LEN>)
      ->DenseRange(0, 1000, 200)
      ->Iterations(num_iter);
  RegisterBenchmark("open_file_size", bench<Mode::OPEN_FILE_SIZE>)
      ->RangeMultiplier(2)
      ->Range(2 * 1024 * 1024, 1024 * 1024 * 1024)
      ->Iterations(num_iter);

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
