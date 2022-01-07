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

  unlink(FILEPATH);

  fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  char buf[8];
  for (int i = 0; i < num_tx; ++i) write(fd, buf, sizeof(buf));
  close(fd);

  if constexpr (mode == Mode::OPEN_CLOSE) {
    for (auto _ : state) {
      fd = open(FILEPATH, O_RDONLY);
      close(fd);
    }
  } else if constexpr (mode == Mode::STAT) {
    struct stat st;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    for (auto _ : state) {
      stat(FILEPATH, &st);
    }
  } else if constexpr (mode == Mode::FSTAT) {
    fd = open(FILEPATH, O_RDONLY);
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
    bm->RangeMultiplier(10)->Range(1, 10000)->Iterations(num_iter);
  }

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
