ULAYFS_PATH_DEBUG:=$(shell readlink -e ../build-debug/libulayfs.so)
ULAYFS_PATH_RELEASE:=$(shell readlink -e ../build-release/libulayfs.so)

.PHONY: all
all:
	echo "Please use make [debug|release|posix]"
	@exit 1

.PHONY: debug release posix
debug:
	$(MAKE) ULAYFS_PATH=${ULAYFS_PATH_DEBUG} leveldb-test
release:
	$(MAKE) ULAYFS_PATH=${ULAYFS_PATH_RELEASE} leveldb-test
posix:
	$(MAKE) ULAYFS_PATH= leveldb-test

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
	TEST_TMPDIR=test-temp/ LD_PRELOAD=${ULAYFS_PATH} ./db_test

.PHONY: clean
clean:
	rm -rf leveldb/build
