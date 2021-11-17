.PHONY: debug release asan ubsan clean

asan ubsan: export CC := clang
asan ubsan: export CXX := clang++

debug release asan ubsan:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j -- --quiet

clean:
	rm -rf build-* test.txt
