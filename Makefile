.PHONY: debug release profile pmemcheck
debug release profile pmemcheck: export CC := gcc
debug release profile pmemcheck: export CXX := g++

.PHONY: asan ubsan msan tsan
asan ubsan msan tsan: export CC := clang
asan ubsan msan tsan: export CXX := clang++

debug release profile pmemcheck asan ubsan msan tsan:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@ $(CMAKE_ARGS)
	cmake --build build-$@ -j -- --quiet $(MAKE_ARGS)

.PHONY: clean
clean:
	rm -rf build* test.txt
