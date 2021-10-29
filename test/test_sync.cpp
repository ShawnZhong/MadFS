#include <fcntl.h>

#include <cassert>
#include <iostream>
#include <thread>

#include "lib.h"

constexpr auto FILEPATH = "test.txt";
constexpr auto NUM_BYTES = 32;
constexpr auto BYTES_PER_THREAD = 2;

char const hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

void fill_buff(char* buff, int num_elem, int init = 0) {
  std::generate(buff, buff + num_elem,
                [i = init]() mutable { return hex_chars[i++ % 16]; });
}

int main(int argc, char* argv[]) {
  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  char empty_buf[NUM_BYTES]{};
  pwrite(fd, empty_buf, NUM_BYTES, 0);

  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_BYTES; ++i) {
    threads.emplace_back(std::thread([&]() {
      char buf[BYTES_PER_THREAD]{};
      fill_buff(buf, BYTES_PER_THREAD, i);
      pwrite(fd, buf, BYTES_PER_THREAD, i);
    }));
    // uncomment the line below to run sequentially
    // threads.back().join();
  }

  for (auto& thread : threads) {
    if (thread.joinable()) thread.join();
  }

  char actual[NUM_BYTES]{};
  pread(fd, actual, NUM_BYTES, 0);
  std::cout << actual << "\n";

  char expected[NUM_BYTES];
  fill_buff(expected, NUM_BYTES);

  assert(memcmp(expected, actual, NUM_BYTES) == 0);
}
