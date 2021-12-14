.PHONY: debug release posix
debug: export ULAYFS_PATH := $(shell readlink -e ../build-debug/libulayfs.so)
release: export ULAYFS_PATH := $(shell readlink -e ../build-release/libulayfs.so)
posix: export ULAYFS_PATH :=
debug release posix: leveldb-test

.PHONY: ulayfs
ulayfs:
	make -C .. debug
	make -C .. release

.PHONY: leveldb leveldb-test
leveldb:
	cd leveldb && \
	mkdir -p build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	cmake --build . -j
leveldb-test: ulayfs leveldb
	cd leveldb/build && \
	mkdir -p test-temp && \
	TEST_TMPDIR=test-temp/ LD_PRELOAD=$(ULAYFS_PATH) ./db_test

.PHONY: clean
clean:
	rm -rf leveldb/build
