#include <fcntl.h>

#include <cassert>
#include <iostream>
#include <thread>

#include "common.h"

constexpr auto NUM_THREAD = 32;
constexpr auto NUM_ITER_PER_THREAD = 100;
constexpr auto BYTES_PER_ITER = 4;

int main() {
  [[maybe_unused]] ssize_t ret;

  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char empty_buf[NUM_THREAD]{};
  ret = pwrite(fd, empty_buf, NUM_THREAD, 0);
  assert(ret == NUM_THREAD);

  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_THREAD; ++i) {
    threads.emplace_back(std::thread([&, i]() {
      char buf[BYTES_PER_ITER]{};
      fill_buff(buf, BYTES_PER_ITER, i);
      for (int j = 0; j < NUM_ITER_PER_THREAD; ++j) {
        ssize_t rc = pwrite(fd, buf, BYTES_PER_ITER, i);
        assert(rc == BYTES_PER_ITER);
        rc = fsync(fd);
        assert(rc == 0);
      }
    }));
    // uncomment the line below to run sequentially
    //    threads.back().join();
  }

  for (auto& thread : threads) {
    if (thread.joinable()) thread.join();
  }

  // check result
  {
    char actual[NUM_THREAD]{};
    ret = pread(fd, actual, NUM_THREAD, 0);
    assert(ret == NUM_THREAD);

    char expected[NUM_THREAD];
    fill_buff(expected, NUM_THREAD);

    CHECK_RESULT(expected, actual, NUM_THREAD, fd);
  }

  fsync(fd);
  close(fd);
}
