In this folder, we implement `extern` functions with the same name and signature
as the glibc functions.

The glibc functions are declared as weak symbols, so that they can be overridden
by our own implementation:

```shell
> nm -D /lib/x86_64-linux-gnu/libc.so.6 | grep "fstat@@"
0000000000113e80 W fstat@@GLIBC_2.33
```

The actual glibc functions are loaded in `posix.h` using `dlsym`.

`lib.cpp` is the entry point for the library. It contains the constructor and
destructor functions that are called when the library is loaded and unloaded.
