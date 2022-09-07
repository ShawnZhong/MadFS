gcc_targets := release debug relwithdebinfo profile pmemcheck
clang_targets := asan ubsan msan tsan

.PHONY: $(gcc_targets) $(clang_targets) clean

$(gcc_targets): export CC := $(shell command -v gcc-10 2> /dev/null || echo gcc)
$(gcc_targets): export CXX := $(shell command -v g++-10 2> /dev/null || echo g++)

$(clang_targets): export CC := $(shell command -v clang-10 2> /dev/null || echo clang)
$(clang_targets): export CXX := $(shell command -v clang++-10 2> /dev/null || echo clang++)

BUILD_TARGETS ?= all

$(gcc_targets) $(clang_targets):
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@ $(CMAKE_ARGS)
	cmake --build build-$@ -j --target $(BUILD_TARGETS) -- --quiet $(BUILD_ARGS)

clean:
	rm -rf build* test.txt /dev/shm/ulayfs_*
