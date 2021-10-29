#include <fcntl.h>

#include <cassert>
#include <iostream>
#include <thread>

#include "lib.h"

constexpr auto FILEPATH = "test.txt";
constexpr auto NUM_BYTES = 32;

char const hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

int main(int argc, char* argv[]) {
  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char empty_buf[NUM_BYTES]{};
  pwrite(fd, empty_buf, NUM_BYTES, 0);

  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_BYTES; ++i) {
    threads.emplace_back(std::thread([&]() {
      char src_buf[1]{};
      src_buf[0] = hex_chars[i % 16];
      pwrite(fd, src_buf, 1, i);
    }));
    // uncomment the line below to run sequentially
    //    threads.back().join();
  }

  for (auto& thread : threads) {
    if (thread.joinable()) thread.join();
  }

  char actual[NUM_BYTES]{};
  pread(fd, actual, NUM_BYTES, 0);
  std::cout << actual << "\n";

  char expected[NUM_BYTES];
  std::generate(expected, expected + NUM_BYTES,
                [i = 0]() mutable { return hex_chars[i++ % 16]; });

  assert(memcmp(expected, actual, NUM_BYTES) == 0);
}
