#include <fcntl.h>

#include <iostream>
#include <thread>

#include "common.h"

constexpr auto NUM_BYTES = 128;
constexpr auto BYTES_PER_THREAD = 2;

const char* filepath = get_filepath();

int main() {
  [[maybe_unused]] ssize_t ret;

  unlink(filepath);
  int fd = open(filepath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char empty_buf[NUM_BYTES]{};
  ret = pwrite(fd, empty_buf, NUM_BYTES, 0);
  ASSERT(ret == NUM_BYTES);

  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_BYTES; ++i) {
    threads.emplace_back(std::thread([&, i]() {
      char buf[BYTES_PER_THREAD]{};
      fill_buff(buf, BYTES_PER_THREAD, i);
      ssize_t rc = pwrite(fd, buf, BYTES_PER_THREAD, i);
      ASSERT(rc == BYTES_PER_THREAD);
      rc = fsync(fd);
      ASSERT(rc == 0);
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
    ASSERT(ret == NUM_BYTES);

    char expected[NUM_BYTES];
    fill_buff(expected, NUM_BYTES);

    CHECK_RESULT(expected, actual, NUM_BYTES, fd);
  }

  fsync(fd);
  close(fd);
}
