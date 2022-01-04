#include <fcntl.h>

#include <cassert>
#include <iostream>
#include <thread>

#include "common.h"

constexpr auto NUM_BYTES = 128;
constexpr auto BYTES_PER_THREAD = 2;

int main(int argc, char* argv[]) {
  [[maybe_unused]] ssize_t ret;

  unlink(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char empty_buf[NUM_BYTES]{};
  ret = pwrite(fd, empty_buf, NUM_BYTES, 0);
  assert(ret == NUM_BYTES);

  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_BYTES; ++i) {
    threads.emplace_back(std::thread([&, i]() {
      char buf[BYTES_PER_THREAD]{};
      fill_buff(buf, BYTES_PER_THREAD, i);
      ssize_t rc = pwrite(fd, buf, BYTES_PER_THREAD, i);
      assert(rc == BYTES_PER_THREAD);
      rc = fsync(fd);
      assert(rc == 0);
    }));
    // uncomment the line below to run sequentially
    //    threads.back().join();
  }

  for (auto& thread : threads) {
    if (thread.joinable()) thread.join();
  }

  // check result
  {
    char actual[NUM_BYTES]{};
    ret = pread(fd, actual, NUM_BYTES, 0);
    assert(ret == NUM_BYTES);

    char expected[NUM_BYTES];
    fill_buff(expected, NUM_BYTES);

    CHECK_RESULT(expected, actual, NUM_BYTES, fd);
  }

  fsync(fd);
  close(fd);
}
