.PHONY: debug release
debug release: export CC := gcc
debug release: export CXX := g++

.PHONY: asan ubsan msan
asan ubsan msan: export CC := clang
asan ubsan msan: export CXX := clang++

debug release asan ubsan msan:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j -- --quiet

.PHONY: clean
clean:
	rm -rf build-* test.txt
