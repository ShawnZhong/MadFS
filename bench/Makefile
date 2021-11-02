.PHONY: debug release
debug: export ULAYFS_PATH := ../build-debug/libulayfs.so
release: export ULAYFS_PATH := ../build-release/libulayfs.so
debug release: leveldb-test

.PHONY: ulayfs
ulayfs:
	make -C ..

.PHONY: leveldb leveldb-test
leveldb:
	cd leveldb && \
	mkdir -p build && \
	cd build && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	cmake --build . -j
leveldb-test: ulayfs leveldb
	LD_PRELOAD=$(ULAYFS_PATH) ./leveldb/build/db_test

.PHONY: clean
clean:
	rm -rf leveldb/build
