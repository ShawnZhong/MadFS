.PHONY: release debug clean

release debug:
	mkdir -p build-$@
	cmake -S . -B build-$@ -DCMAKE_BUILD_TYPE=$@
	cmake --build build-$@ -j

clean:
	rm -rf build-*
