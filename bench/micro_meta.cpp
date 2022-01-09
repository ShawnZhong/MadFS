#include <sys/stat.h>

#include "common.h"

int fd;
int num_iter = get_num_iter(1000);

enum class Mode {
  OPEN_CLOSE,
  STAT,
  FSTAT,
};

template <Mode mode>
void bench(benchmark::State& state) {
  const auto num_tx = state.range(0);

  unlink(filepath);

  fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  if (fd < 0) state.SkipWithError("open failed");

  char buf[8];
  for (int i = 0; i < num_tx; ++i) {
    [[maybe_unused]] ssize_t res = write(fd, buf, sizeof(buf));
    assert(res == sizeof(buf));
  }
  close(fd);

  if constexpr (mode == Mode::OPEN_CLOSE) {
    for (auto _ : state) {
      fd = open(filepath, O_RDONLY);
      assert(fd >= 0);
      close(fd);
    }
  } else if constexpr (mode == Mode::STAT) {
    struct stat st;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    for (auto _ : state) {
      stat(filepath, &st);
    }
  } else if constexpr (mode == Mode::FSTAT) {
    fd = open(filepath, O_RDONLY);
    struct stat st;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    for (auto _ : state) {
      fstat(fd, &st);
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  for (auto& bm : {RegisterBenchmark("open_close", bench<Mode::OPEN_CLOSE>),
                   RegisterBenchmark("stat", bench<Mode::STAT>),
                   RegisterBenchmark("fstat", bench<Mode::FSTAT>)}) {
    bm->DenseRange(100, 1000, 100)->Iterations(num_iter);
  }

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
