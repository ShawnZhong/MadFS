#include "common.h"

constexpr int MIN_SIZE = 8;
constexpr int MAX_SIZE = 128 * 1024;

int fd;
int num_iter = get_num_iter();

enum class Mode {
  APPEND,
  SEQ_READ,
  SEQ_WRITE,
  RND_READ,
  RND_WRITE,
};

template <Mode mode>
void bench(benchmark::State& state) {
  const auto num_bytes = state.range(0);

  [[maybe_unused]] char dst_buf[MAX_SIZE];
  [[maybe_unused]] char src_buf[MAX_SIZE];
  std::fill(src_buf, src_buf + MAX_SIZE, 'x');

  unlink(filepath);

  // preallocate file
  fd = open(filepath, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
  if (fd < 0) state.SkipWithError("open failed");
  if constexpr (mode != Mode::APPEND) {
    auto len = num_bytes * num_iter;
    auto buf = new char[len];
    std::fill(buf, buf + len, 'x');
    [[maybe_unused]] ssize_t res = write(fd, buf, len);
    assert(res == len);
    fsync(fd);
    delete[] buf;
  }
  close(fd);

  int open_flags = 0;
  if constexpr (mode == Mode::SEQ_READ || mode == Mode::RND_READ)
    open_flags = O_RDONLY;
  else if constexpr (mode == Mode::SEQ_WRITE || mode == Mode::RND_WRITE)
    open_flags = O_RDWR;
  else if constexpr (mode == Mode::APPEND)
    open_flags = O_RDWR | O_APPEND;

  fd = open(filepath, open_flags);
  if (fd < 0) state.SkipWithError("open failed");

  // run benchmark
  if constexpr (mode == Mode::APPEND || mode == Mode::SEQ_WRITE) {
    for (auto _ : state) {
      [[maybe_unused]] ssize_t res = write(fd, src_buf, num_bytes);
      assert(res == num_bytes);
      fsync(fd);
    }
  } else if constexpr (mode == Mode::SEQ_READ) {
    for (auto _ : state) {
      [[maybe_unused]] ssize_t res = read(fd, dst_buf, num_bytes);
      assert(res == num_bytes);
      assert(memcmp(dst_buf, src_buf, num_bytes) == 0);
    }
  } else if constexpr (mode == Mode::RND_READ || mode == Mode::RND_WRITE) {
    // prepare random offset
    int rand_off[num_iter];
    std::generate(rand_off, rand_off + num_iter,
                  [&]() { return rand() % num_iter * num_bytes; });

    int i = 0;
    if constexpr (mode == Mode::RND_READ) {
      for (auto _ : state) {
        [[maybe_unused]] ssize_t res =
            pread(fd, dst_buf, num_bytes, rand_off[i++]);

        assert(res == num_bytes);
        if (memcmp(dst_buf, src_buf, num_bytes) != 0) {
          fprintf(stderr, "dst_buf = %s\n", dst_buf);
          fprintf(stderr, "src_buf = %s\n", src_buf);
          assert(false);
        }
        assert(memcmp(dst_buf, src_buf, num_bytes) == 0);
      }
    } else if constexpr (mode == Mode::RND_WRITE) {
      for (auto _ : state) {
        [[maybe_unused]] ssize_t res =
            pwrite(fd, src_buf, num_bytes, rand_off[i++]);
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
  close(fd);
  unlink(filepath);
}

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  for (auto& bm : {
           RegisterBenchmark("seq_read", bench<Mode::SEQ_READ>),
           RegisterBenchmark("rnd_read", bench<Mode::RND_READ>),
           RegisterBenchmark("seq_write", bench<Mode::SEQ_WRITE>),
           RegisterBenchmark("rnd_write", bench<Mode::RND_WRITE>),
           RegisterBenchmark("append", bench<Mode::APPEND>),
       }) {
    bm->RangeMultiplier(2)->Range(MIN_SIZE, MAX_SIZE)->Iterations(num_iter);
  }

  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
