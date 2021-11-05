#pragma once

#include <algorithm>

constexpr char FILEPATH[] = "test.txt";
constexpr char TEST_STR[] = "test str\n";
constexpr int TEST_STR_LEN = sizeof(TEST_STR) - 1;

char const hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

void fill_buff(char* buff, int num_elem, int init = 0) {
  std::generate(buff, buff + num_elem,
                [i = init]() mutable { return hex_chars[i++ % 16]; });
}
