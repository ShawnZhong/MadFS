.PHONY: all
all:
	echo "Please use make [debug|release|posix|ycsb]"
	@exit 1

.PHONY: debug release posix
debug:
	$(MAKE) -C .. debug
	$(MAKE) ULAYFS_PATH=$(shell readlink -e ../build-debug/libulayfs.so) leveldb-test
release:
	$(MAKE) -C .. release
	$(MAKE) ULAYFS_PATH=$(shell readlink -e ../build-release/libulayfs.so) leveldb-test
posix:
	$(MAKE) ULAYFS_PATH= leveldb-test
ycsb:
	$(MAKE) -C .. release
	$(MAKE) -C ycsbcli
	$(MAKE) ULAYFS_PATH=$(shell readlink -e ../build-release/libulayfs.so) leveldb-ycsb

.PHONY: leveldb leveldb-test
leveldb:
	cd leveldb && \
	mkdir -p build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	cmake --build . -j
leveldb-test: leveldb
	cd leveldb/build && \
	mkdir -p test-temp && \
	TEST_TMPDIR=test-temp/ LD_PRELOAD=${ULAYFS_PATH} ./db_test --gtest_filter=-DBTest.Randomized
leveldb-ycsb: leveldb
	cd ycsbcli && \
	mkdir -p bench-dbdir && \
	python3 ycsbrun.py bench-dbdir

.PHONY: clean
clean:
	rm -rf leveldb/build ycsbcli/bench-dbdir
	$(MAKE) -C ycsbcli clean

