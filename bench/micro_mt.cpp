#include <benchmark/benchmark.h>
#include <sys/stat.h>

#include "common.h"
#include "debug.h"
#include "zipf.h"

#ifdef NDEBUG
constexpr static bool debug = false;
#else
constexpr static bool debug = true;
#endif

constexpr int BLOCK_SIZE = 4096;
constexpr int MAX_NUM_THREAD = 16;

const char* filepath = get_filepath();
int num_iter = get_num_iter();
int file_size = get_file_size();

int fd;

enum class Mode {
  UNIF,
  APPEND,
  ZIPF,
};

template <Mode mode>
void bench(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  pin_core(state.thread_index);

  [[maybe_unused]] char dst_buf[BLOCK_SIZE * MAX_NUM_THREAD];
  [[maybe_unused]] char src_buf[BLOCK_SIZE * MAX_NUM_THREAD];
  std::fill(src_buf, src_buf + BLOCK_SIZE * MAX_NUM_THREAD, 'x');

  if (state.thread_index == 0) {
    if constexpr (mode == Mode::APPEND) {
      unlink(filepath);
    }

    fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) state.SkipWithError("open failed");

    // preallocate file
    if constexpr (mode != Mode::APPEND) {
      // check file size
      struct stat st;  // NOLINT(cppcoreguidelines-pro-type-member-init)
      fstat(fd, &st);
      if (st.st_size != file_size) {
        if (st.st_size != 0) {
          state.SkipWithError("file size is not 0");
        }
        prefill_file(fd, file_size);
      }
    }
  }

  if (ulayfs::is_linked()) ulayfs::debug::clear_counts();

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
  } else if constexpr (mode == Mode::UNIF) {
    const auto read_percent = state.range(1);

    bool is_read[num_iter];
    std::generate(is_read, is_read + num_iter,
                  [&]() { return rand() % 100 < read_percent; });

    int rand_off[num_iter];
    std::generate(rand_off, rand_off + num_iter, [&]() {
      return rand() % (file_size / num_bytes) * num_bytes;
    });

    int i = 0;
    for (auto _ : state) {
      if (is_read[i]) {
        [[maybe_unused]] ssize_t res =
            pread(fd, dst_buf, num_bytes, rand_off[i]);
        assert(res == num_bytes);
        assert(memcmp(dst_buf, src_buf, num_bytes) == 0);
      } else {
        [[maybe_unused]] ssize_t res =
            pwrite(fd, src_buf, num_bytes, rand_off[i]);
        assert(res == num_bytes);
        fsync(fd);
      }
      i++;
    }
  } else if constexpr (mode == Mode::ZIPF) {
    const double theta = state.range(1) / 100.0;

    std::mt19937 generator(state.thread_index);
    zipf_distribution<int> zipf(file_size / BLOCK_SIZE, theta);

    off_t offsets[num_iter];
    std::generate(offsets, offsets + num_iter,
                  [&]() { return zipf(generator) - 1; });

    int i = 0;
    for (auto _ : state) {
      [[maybe_unused]] ssize_t res =
          pwrite(fd, src_buf, num_bytes, offsets[i++] * BLOCK_SIZE);
      assert(res == num_bytes);
      fsync(fd);
    }
  }

  // tear down
  if (state.thread_index == 0) {
    close(fd);
  }

  // report result
  auto items_processed = static_cast<int64_t>(state.iterations());
  auto bytes_processed = items_processed * num_bytes;
  state.SetBytesProcessed(bytes_processed);
  state.SetItemsProcessed(items_processed);

  if (ulayfs::is_linked()) {
    double start_cnt =
        ulayfs::debug::get_count(ulayfs::Event::SINGLE_BLOCK_TX_START) +
        ulayfs::debug::get_count(ulayfs::Event::ALIGNED_TX_COPY);
    double copy_cnt =
        ulayfs::debug::get_count(ulayfs::Event::SINGLE_BLOCK_TX_COPY);
    double commit_cnt =
        ulayfs::debug::get_count(ulayfs::Event::SINGLE_BLOCK_TX_COMMIT) +
        ulayfs::debug::get_count(ulayfs::Event::ALIGNED_TX_COMMIT);

    if (start_cnt != 0 && commit_cnt != 0) {
      state.counters["tx_copy"] = copy_cnt / start_cnt / state.threads;
      state.counters["tx_commit"] = commit_cnt / start_cnt / state.threads;
    }
  }
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  unlink(filepath);

  for (const auto& read_percent : {0, 50, 95, 100}) {
    auto name = std::string("unif_") + std::to_string(read_percent) + "R";
    benchmark::RegisterBenchmark(name.c_str(), bench<Mode::UNIF>)
        ->Args({BLOCK_SIZE, read_percent})
        ->DenseThreadRange(1, MAX_NUM_THREAD)
        ->Iterations(num_iter)
        ->UseRealTime();
  }

  for (const auto& [name, num_bytes] :
       {std::pair{"zipf_4k", 4096}, std::pair{"zipf_2k", 2048}}) {
    benchmark::RegisterBenchmark(name, bench<Mode::ZIPF>)
        ->Args({num_bytes, 90})
        ->DenseThreadRange(1, MAX_NUM_THREAD)
        ->Iterations(num_iter)
        ->UseRealTime();
  }

  //  for (long theta_x100 = 0; theta_x100 <= 160; theta_x100 += 10) {
  //    benchmark::RegisterBenchmark("zipf_4k", bench<Mode::ZIPF>)
  //        ->Args({BLOCK_SIZE, theta_x100})
  //        ->Threads(8)
  //        ->Iterations(num_iter)
  //        ->UseRealTime();
  //  }

  for (const auto& [name, num_bytes] :
       {std::pair{"append_4k", 4096}, std::pair{"append_2k", 2048}}) {
    benchmark::RegisterBenchmark(name, bench<Mode::APPEND>)
        ->Args({num_bytes})
        ->DenseThreadRange(1, MAX_NUM_THREAD)
        ->Iterations(num_iter)
        ->UseRealTime();
  }

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
