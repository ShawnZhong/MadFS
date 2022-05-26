gcc_targets := release debug relwithdebinfo profile pmemcheck
clang_targets := asan ubsan msan tsan

.PHONY: $(gcc_targets) $(clang_targets) clean

$(gcc_targets): export CC := gcc
$(gcc_targets): export CXX := g++

$(clang_targets): export CC := clang
$(clang_targets): export CXX := clang++

$(gcc_targets) $(clang_targets):
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@ $(CMAKE_ARGS)
	cmake --build build-$@ -j --target $(BUILD_TARGETS) -- --quiet $(BUILD_ARGS)

clean:
	rm -rf build* test.txt /dev/shm/ulayfs_*
