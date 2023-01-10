/**
 * Perform garbage collection on a file.
 */
#include "gc.h"

#include <iostream>

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
    return 1;
  }

  const char *filename = argv[1];
  ulayfs::utility::GarbageCollector garbage_collector(filename);
  bool is_done = garbage_collector.do_gc();
  std::cout << "GC_done=" << is_done << std::endl;

  return 0;
}
