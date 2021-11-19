.PHONY: debug release
debug release: export CC := gcc
debug release: export CXX := g++

.PHONY: asan ubsan msan tsan
asan ubsan msan tsan: export CC := clang
asan ubsan msan tsan: export CXX := clang++

debug release asan ubsan msan tsan:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j -- --quiet

.PHONY: clean
clean:
	rm -rf build-* test.txt
