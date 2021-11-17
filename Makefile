.PHONY: debug release asan ubsan msan clean

asan ubsan msan: export CC := clang
asan ubsan msan: export CXX := clang++

debug release asan ubsan msan:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j -- --quiet

clean:
	rm -rf build-* test.txt
