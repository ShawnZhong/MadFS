#include "../src/futex.h"

#include <sys/mman.h>
#include <sys/wait.h>

using namespace ulayfs;

int main() {
  auto futex =
      static_cast<Futex *>(mmap(NULL, sizeof(Futex), PROT_READ | PROT_WRITE,
                                MAP_ANONYMOUS | MAP_SHARED, -1, 0));
  futex->init();

  int rc = fork();
  if (rc == 0) {  // child
    printf("child acquire start\n");
    futex->acquire();
    printf("child acquire end\n");

    sleep(2);

    exit(0);  // note this exit
    printf("child release\n");
    futex->release();
  } else {  // parent
    sleep(1);

    printf("parent acquire start\n");
    futex->acquire();
    printf("parent acquire end\n");

    printf("parent release\n");
    futex->release();

    wait(NULL);
    exit(0);
  }
}
