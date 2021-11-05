#include <fcntl.h>

#include <cassert>
#include <iostream>
#include <thread>

#include "common.h"
#include "lib.h"

constexpr auto NUM_BYTES = 32;
constexpr auto BYTES_PER_THREAD = 2;

int main(int argc, char* argv[]) {
  ssize_t ret;

  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char empty_buf[NUM_BYTES]{};
  ret = pwrite(fd, empty_buf, NUM_BYTES, 0);
  assert(ret == NUM_BYTES);

  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_BYTES; ++i) {
    threads.emplace_back(std::thread([&]() {
      char buf[BYTES_PER_THREAD]{};
      fill_buff(buf, BYTES_PER_THREAD, i);
      ret = pwrite(fd, buf, BYTES_PER_THREAD, i);
      assert(ret == BYTES_PER_THREAD);
    }));
    // uncomment the line below to run sequentially
    // threads.back().join();
  }

  for (auto& thread : threads) {
    if (thread.joinable()) thread.join();
  }

  char actual[NUM_BYTES]{};
  ret = pread(fd, actual, NUM_BYTES, 0);
  assert(ret == NUM_BYTES);
  std::cout << actual << "\n";

  char expected[NUM_BYTES];
  fill_buff(expected, NUM_BYTES);

  assert(memcmp(expected, actual, NUM_BYTES) == 0);
}
