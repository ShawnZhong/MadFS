#include <sys/mman.h>

#include "../src/futex.h"

using namespace ulayfs;

int main() {
  auto futex =
      static_cast<Futex *>(mmap(NULL, sizeof(Futex), PROT_READ | PROT_WRITE,
                                MAP_ANONYMOUS | MAP_SHARED, -1, 0));
  futex->init();

  int rc = fork();
  if (rc == 0) {  // child
    futex->acquire();
    exit(0);
  } else {  // parent
    sleep(1);
    futex->acquire();
    exit(0);
  }
}
