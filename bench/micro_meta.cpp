#include "common.h"

enum class Mode {
  OPEN_CLOSE,
};

template <Mode mode>
void bench(benchmark::State& state) {
  unlink(FILEPATH);
  close(open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR));
  for (auto _ : state) close(open(FILEPATH, O_RDWR));
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  RegisterBenchmark("open_close", bench<Mode::OPEN_CLOSE>)
      ->Iterations(num_iter);

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
