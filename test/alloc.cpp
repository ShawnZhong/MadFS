#include <fcntl.h>

#include <cassert>
#include <iostream>

#include "../src/lib.h"

constexpr auto FILEPATH = "test.txt";
constexpr auto NUM_BYTES = 64 * 2;
constexpr auto OFFSET = 4096 - 64;

char const hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

int main(int argc, char* argv[]) {
  remove(FILEPATH);
  int fd = open(FILEPATH, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  auto file = ulayfs::files[fd];
  std::cerr << *file << "\n";

  char src_buf[NUM_BYTES];
  std::generate(src_buf, src_buf + NUM_BYTES,
                [i = 0]() mutable { return hex_chars[i++ % 16]; });
  pwrite(fd, src_buf, NUM_BYTES, OFFSET);

  std::cerr << *file << "\n";

  char dst_buf[NUM_BYTES]{};
  pread(fd, dst_buf, NUM_BYTES, OFFSET);

  std::cout << std::string(src_buf, NUM_BYTES) << "\n";
  std::cout << std::string(dst_buf, NUM_BYTES) << "\n";

  assert(memcmp(src_buf, dst_buf, NUM_BYTES) == 0);
}
