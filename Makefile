.PHONY: release debug clean

debug release:
	mkdir -p build-$@
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j

clean:
	rm -rf build-*
