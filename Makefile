.PHONY: debug release asan clean

asan: export CC := clang
asan: export CXX := clang++

debug release asan:
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j -- --quiet

clean:
	rm -rf build-* test.txt
