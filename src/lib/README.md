This folder is the main entrance of the library.

`lib.cpp` contains the constructor and destructor functions that are called when
the library is loaded and unloaded.

The rest of the `.cpp` files implement `extern` functions with the same name
and signature as the glibc functions. They act as a thin wrapper around the
`class File` in `src/file/`.

The glibc functions are declared as weak symbols:

```shell
> nm -D /lib/x86_64-linux-gnu/libc.so.6 | grep "fstat@@"
0000000000113e80 W fstat@@GLIBC_2.33
```

So they can be overridden by our own implementation in this folder.

Note that the actual glibc functions are loaded in `posix.h` using `dlsym`.


